/*
 * IVrixDB software is licensed under the IVrixDB Software License Agreement
 * (the "License"); you may not use this file or the IVrixDB except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     https://github.com/hefty111/IVrixDB/blob/master/LICENSE.pdf
 *
 * Unless required by applicable law or agreed to in writing, IVrixDB software is provided
 * and distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions
 * and limitations under the License. See the NOTICE file distributed with the IVrixDB Software
 * for additional information regarding copyright ownership.
 */

package org.apache.solr.ivrixdb.core.overseer.state;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.utilities.*;
import org.slf4j.Logger;

import static org.apache.solr.ivrixdb.utilities.Constants.IVrix.Limitations.State.*;

/**
 * This object is responsible for storing a cache of IVrixDB's overall
 * state, and holding helper functions that modify and read that state.
 * IVrixDB's state is mainly comprised of IVrix Indexes, which are comprised of
 * IVrix Buckets. Some of the state is persistable, and some isn't.
 *
 * To learn more about IVrix Indexes and IVrix Buckets, please refer to
 * {@link IVrixBucket} and {@link IVrixIndex}.
 *
 * (IVrixState implements the fault-tolerant principles expressed in IVrixOverseer
 * {@link IVrixOverseer}).
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: refactoring idea -- The main issue is that this class is supposed to be the
 *                           one that solely modifies the state. however, IVrixBucket
 *                           and IVrixIndex are given the power to also modify the state,
 *                           for convenience reasons. This kind-of spreads that responsibility
 *                           across all three classes...find a way to encapsulate that responsibility.
 *
 * TODO: refactoring idea -- create a uniform all-buckets iterator.
 *
 * TODO: refactoring idea -- there are way too many loops all the buckets. Try to merge functions together to avoid looping so many times.
 *
 * TODO: refactor idea -- update any function name that does not 100% align and represent the javadoc,
 *                        because that means that the name isn't a good one :/
 *
 * TODO: refactor idea -- rename functions "deleteAnyDeadAndUnusableHotBuckets", "completeAllUnfinishedWarmToColdRollover",
 *                        "ensureAttachAndDetachStateConsistency" to something more like "recoverFromAnyFailed___Operation".
 */
public class IVrixState {
  private static final Logger log = IVrixOverseer.log;

  private final ReentrantLock stateLock = new ReentrantLock();
  private final Object updateNotifier = new Object();
  private final Map<String, IVrixIndex> indexMap;

  /**
   * Creates an empty state
   */
  public IVrixState() {
    this.indexMap = new HashMap<>();
  }

  /**
   * Loads the persistent properties of the state from Zookeeper,
   * and has non-persistent properties at default values
   */
  public void loadFromZK() throws IOException {
    log.info("Loading IVrix state from ZK...");

    IVrixStateZkUtil.ensurePersistentStatePathExists();
    List<String> indexNames = IVrixStateZkUtil.retrieveAllPersistedIndexes();

    for (String indexName : indexNames) {
      indexMap.put(indexName, IVrixIndex.loadFromZK(indexName));
    }
  }

  /**
   * Ensures consistency between IVrixDB and Solr states,
   * Recovery from non-completed/failed operations,
   * and adherence to the limitation constraints per node.
   *
   * failed CREATE-BUCKET operations delete the partially made buckets,
   * failed ROLL-TO-COLD operations fully roll the buckets to COLD, and
   * failed ATTACH/DETACH operations fully ATTACH or DETACH the buckets.
   */
  public void ensureStateConsistency() throws IOException {
    log.info("Ensuring IVrixDB state is consistent with physical state...");

    deleteAnyDeadAndUnusableHotBuckets();
    completeAllUnfinishedWarmToColdRollover();
    ensureAttachAndDetachStateConsistency();

    ensureHotAndWarmBucketsDoNotExceedLimit();
    ensureColdBucketsDoNotExceedLimit();
  }

  /**
   * Rolls any HOT bucket from all non-live IVrix Nodes to WARM
   */
  public void freeHotBucketsFromDownedNodes() throws IOException {
    log.info("Freeing all hot buckets from nodes that are currently down..");

    Collection<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    for (IVrixIndex index : getAllIndexes()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        if (bucket.isHot() && !liveNodes.contains(bucket.getIndexerNodeName())) {
          bucket.rollToWarm();
        }
      }
    }
  }

  /**
   * Rolls any HOT bucket from an IVrix Node to WARM
   */
  public void freeHotBucketsFromNode(String nodeName) throws IOException {
    log.info("Freeing all hot buckets from node -- " + nodeName + " ...");

    for (IVrixIndex index : getAllIndexes()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        if (bucket.isHot() && Objects.equals(bucket.getIndexerNodeName(), nodeName)) {
          bucket.rollToWarm();
        }
      }
    }
  }

  /**
   * Removes any bucket holders created from an IVrix Node
   */
  public void removeAllHoldersFromNode(String nodeName) {
    log.info("Removing all bucket holders initialized by node -- " + nodeName + " ...");

    for (IVrixIndex index : indexMap.values()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        bucket.removeAllHoldersFromNode(nodeName);
      }
    }
  }



  private void deleteAnyDeadAndUnusableHotBuckets() throws IOException {
    log.info("Ensuring that there are no dead or unusable hot buckets...");

    for (IVrixIndex index : indexMap.values()) {
      Iterator<IVrixBucket> bucketIterator = index.getBuckets().iterator();
      while (bucketIterator.hasNext()) {
        IVrixBucket bucket = bucketIterator.next();

        if (bucket.isSolrMetadataNotFilled() && bucket.isHot()) {
          IVrixBucket.delete(bucket);
          bucketIterator.remove();
        }
      }
    }
  }

  private void ensureAttachAndDetachStateConsistency() throws IOException {
    log.info("Ensuring attach/detach states are consistent between Solr and IVrixDB...");

    for (IVrixIndex index : getAllIndexes()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        if (bucket.isDetached() && !bucket.isDetachedOnAllRelevantLiveNodes()) {
          bucket.detach();
        } else if (bucket.isAttached() && !bucket.isAttachedOnAllRelevantLiveNodes()) {
          bucket.attach();
        }
      }
    }
  }

  private void completeAllUnfinishedWarmToColdRollover() throws IOException {
    log.info("Ensuring all Warm-to-Cold rollover has been completed...");

    for (IVrixIndex index : getAllIndexes()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        boolean isPreparedForColdRollover = bucket.getSolrMetadata().getReplicas().size() == 1;
        if (bucket.isCold() && !isPreparedForColdRollover) {
          bucket.rollToCold();
        }
      }
    }
  }

  private void ensureHotAndWarmBucketsDoNotExceedLimit() throws IOException {
    log.info("Ensuring Hot and Warm buckets do not exceed specified count limit...");

    Set<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    for (String liveNode : liveNodes) {
      int numberOfBucketsToRoll = countNumberOfAttachedHotAndWarm(liveNode) - MAXIMUM_ATTACHED_HOT_PLUS_WARM_BUCKETS_PER_NODE;

      while (numberOfBucketsToRoll > 0) {
        IVrixBucket bucketToRollToCold = getWarmBucketToRollToCold(liveNode);
        bucketToRollToCold.rollToCold();
        numberOfBucketsToRoll -= 1;
      }
    }
  }

  private void ensureColdBucketsDoNotExceedLimit() throws IOException {
    log.info("Ensuring attached Cold buckets do not exceed specified count limit...");

    Set<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    for (String liveNode : liveNodes) {
      ensureColdBucketsDoNotExceedLimitInNode(liveNode);
    }
  }



  /**
   * @return A list of live IVrix Node Names for replicating a bucket
   *          (of length no longer than [replicationFactor - 1])
   */
  public List<String> getNodesForBucketReplication(String indexerNodeName, int replicationFactor) {
    List<String> potentialNodesForReplication = new LinkedList<>(IVrixLocalNode.getClusterState().getLiveNodes());
    potentialNodesForReplication.remove(indexerNodeName);

    if (potentialNodesForReplication.size() > replicationFactor) {
      Collections.shuffle(potentialNodesForReplication, new Random());
      potentialNodesForReplication = potentialNodesForReplication.subList(0, replicationFactor);
    }
    return potentialNodesForReplication;
  }

  /**
   * Creates an IVrix Bucket and stores it into an IVrix Index
   */
  public void createIVrixBucket(String indexName, String bucketName, String indexerNodeName,
                                List<String> nodesForReplication, int replicationFactor) throws Exception {
    IVrixBucket bucket = new IVrixBucket(indexName, bucketName, indexerNodeName);
    indexMap.get(indexName).addBucket(bucket);
    bucket.createSelf(nodesForReplication, replicationFactor);
  }

  /**
   * Rolls as many WARM buckets from an IVrix Node in order to make room for a new bucket.
   * Potentially reduces the WARM-bucket-count of other IVrix Nodes (due to replication factor).
   */
  public void makeRoomInNodeForNewBucketIfNecessary(String nodeName) throws IOException {
    int numberOfBucketsToRoll = 1 + countNumberOfAttachedHotAndWarm(nodeName) - MAXIMUM_ATTACHED_HOT_PLUS_WARM_BUCKETS_PER_NODE;

    while (numberOfBucketsToRoll > 0) {
      IVrixBucket bucketToRollToCold = getWarmBucketToRollToCold(nodeName);
      if (bucketToRollToCold.isBeingHeld()) {
        bucketToRollToCold.forceRemoveAllHolders();
      }
      bucketToRollToCold.rollToCold();
      numberOfBucketsToRoll -= 1;
    }
  }

  /**
   * Detaches as many COLD buckets from an IVrix Node in order to ensure that the limitation constraints are met
   */
  public void ensureColdBucketsDoNotExceedLimitInNode(String nodeName) throws IOException {
    int numberOfBucketsToDetach = countNumberOfAttachedCold(nodeName) - MAXIMUM_ATTACHED_COLD_BUCKETS_PER_NODE;

    while (numberOfBucketsToDetach > 0) {
      IVrixBucket coldBucketToDetach = getColdBucketToDetach(nodeName);
      coldBucketToDetach.detach();
      numberOfBucketsToDetach -= 1;
    }
  }



  /**
   * @return A list of the IVrix Nodes that an IVrix Bucket resides in that have reached their max-attached COLD limitation.
   */
  public Set<String> getResidingNodesThatReachedMaxAttachedCold(IVrixBucket bucketToAttach) {
    Set<String> nodesThatReachedMaximumAttachedCold = new HashSet<>();
    for (String residingNode : bucketToAttach.getResidingNodes()) {
      if (this.hasReachedMaximumAttachedCold(residingNode)) {
        nodesThatReachedMaximumAttachedCold.add(residingNode);
      }
    }
    return nodesThatReachedMaximumAttachedCold;
  }

  /**
   * @return A list of IVrix Buckets that are available to detach across a set of IVrix Nodes
   */
  public Set<IVrixBucket> getBucketsToDetach(Set<String> nodeNames) {
    Set<IVrixBucket> bucketsToDetach = new HashSet<>();
    for (String nodeName : nodeNames) {
      IVrixBucket bucketToDetach = this.getColdBucketToDetach(nodeName);
      if (bucketToDetach != null) {
        bucketsToDetach.add(bucketToDetach);
      }
    }
    return bucketsToDetach;
  }

  /**
   * Sets the states to detached and attached for the respective buckets in state only (i.e, marked and not yet operated on)
   */
  public void setStatesToDetachAndAttach(Set<IVrixBucket> bucketsToDetach,
                                          IVrixBucket bucketToAttach) throws IOException {
    for (IVrixBucket bucketToDetach : bucketsToDetach) {
      bucketToDetach.detachInStateOnly();
      bucketToDetach.signalDetachInProcess();
    }
    bucketToAttach.attachInStateOnly();
    bucketToAttach.signalAttachInProcess();
  }

  /**
   * Physically detaches and attaches the respective buckets (i.e, they were marked, and now they are being operated on)
   */
  public void physicallyDetachAndAttach(Set<IVrixBucket> bucketsToDetach,
                                         IVrixBucket bucketToAttach) throws IOException {
    for (IVrixBucket bucketToDetach : bucketsToDetach) {
      bucketToDetach.physicallyDetachOnly();
      bucketToDetach.signalDetachIsFinished();
    }
    bucketToAttach.physicallyAttachOnly();
    bucketToAttach.signalAttachIsFinished();
  }



  private boolean hasReachedMaximumAttachedHotAndWarm(String nodeName) {
    return countNumberOfAttachedHotAndWarm(nodeName) >= MAXIMUM_ATTACHED_HOT_PLUS_WARM_BUCKETS_PER_NODE;
  }

  private boolean hasReachedMaximumAttachedCold(String nodeName) {
    return countNumberOfAttachedCold(nodeName) >= MAXIMUM_ATTACHED_COLD_BUCKETS_PER_NODE;
  }

  private int countNumberOfAttachedHotAndWarm(String nodeName) {
    int numberOfBucketsAttached = 0;
    for (IVrixIndex index : indexMap.values()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        boolean isHotOrWarm = bucket.isWarm() || bucket.isHot();
        boolean doesHaveAnyReplicaInSpecifiedNode = bucket.getCurrentlyResidingNodes().contains(nodeName);

        if (isHotOrWarm && doesHaveAnyReplicaInSpecifiedNode) {
          numberOfBucketsAttached += 1;
        }
      }
    }
    return numberOfBucketsAttached;
  }

  private int countNumberOfAttachedCold(String nodeName) {
    int numberOfBucketsAttached = 0;
    for (IVrixIndex index : indexMap.values()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        boolean isAttached = bucket.isAttached();
        boolean isCold = bucket.isCold();
        boolean doesHaveAnyReplicaInSpecifiedNode = bucket.getResidingNodes().contains(nodeName);

        if (isAttached && isCold && doesHaveAnyReplicaInSpecifiedNode) {
          numberOfBucketsAttached += 1;
        }
      }
    }
    return numberOfBucketsAttached;
  }

  private IVrixBucket getWarmBucketToRollToCold(String nodeName) {
    IVrixBucket warmBucketToRoll = null;

    for (IVrixIndex index : indexMap.values()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        if (bucket.getTimeBounds() == null) {
          continue;
        }

        boolean doesHaveAnyReplicaInSpecifiedNode = bucket.getCurrentlyResidingNodes().contains(nodeName);
        if (bucket.isWarm() && doesHaveAnyReplicaInSpecifiedNode) {
          if (warmBucketToRoll == null) {
            warmBucketToRoll = bucket;

          } else if (bucket.getLongRepresentationOfTimeBounds() < warmBucketToRoll.getLongRepresentationOfTimeBounds()) {
            warmBucketToRoll = bucket;
          }
        }
      }
    }
    return warmBucketToRoll;
  }

  private IVrixBucket getColdBucketToDetach(String nodeName) {
    IVrixBucket coldBucketToDetach = null;

    for (IVrixIndex index : indexMap.values()) {
      for (IVrixBucket bucket : index.getBuckets()) {
        if (bucket.getTimeBounds() == null) {
          continue;
        }

        boolean doesHaveAnyReplicaInSpecifiedNode = bucket.getResidingNodes().contains(nodeName);
        if (bucket.isCold() && bucket.isAttached() && !bucket.isBeingHeld() && doesHaveAnyReplicaInSpecifiedNode) {
          if (coldBucketToDetach == null) {
            coldBucketToDetach = bucket;

          } else if (bucket.getLongRepresentationOfTimeBounds() < coldBucketToDetach.getLongRepresentationOfTimeBounds()) {
            coldBucketToDetach = bucket;
          }
        }
      }
    }
    return coldBucketToDetach;
  }



  /**
   * Creates an IVrix Index and stores it into state
   */
  public void createIVrixIndex(String indexName) throws IOException {
    IVrixIndex newIndex = IVrixIndex.create(indexName);
    indexMap.put(indexName, newIndex);
  }

  /**
   * Deletes an IVrix Index from the state
   */
  public void deleteIVrixIndex(String indexName) throws IOException {
    for (IVrixBucket bucket : getIndex(indexName).getBuckets()) {
      IVrixBucket.delete(bucket);
    }
    IVrixIndex.delete(indexName);
    indexMap.remove(indexName);
  }

  /**
   * @return An IVrix Index with the requested name
   */
  public IVrixIndex getIndex(String indexName) {
    return indexMap.get(indexName);
  }

  /**
   * @return A list of all the IVrix Indexes in the state
   */
  public Collection<IVrixIndex> getAllIndexes() {
    return indexMap.values();
  }

  /**
   * @return A list of all the names of the IVrix Indexes in the state
   */
  public Collection<String> getAllIndexNames() {
    return indexMap.keySet();
  }



  /**
   * Holds the global state lock and prevents any other state-changing operations from executing
   */
  public void acquireLock() {
    stateLock.lock();
  }

  /**
   * Releases the global state lock and allows other state-changing operations to execute
   */
  public void releaseLock() {
    stateLock.unlock();
  }

  /**
   * Releases the global state lock if the thread/operation has a hold on it
   */
  public void releaseLockIfAcquired() {
    if (stateLock.isHeldByCurrentThread()) {
      stateLock.unlock();
    }
  }



  /**
   * Waits for hold/release of any IVrix Bucket to occur
   */
  public void waitForUpdate(IVrixBucket bucketToAttach) throws IOException {
    log.info("Cannot make enough room to attach " + bucketToAttach.getName() + ". Will try again once overall state changes...");
    synchronized (updateNotifier) {
      try {
        updateNotifier.wait();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Notifies all waiters that a hold/release on an IVrix Bucket has occurred
   */
  public void notifyUpdateWaiters() {
    synchronized (updateNotifier) {
      updateNotifier.notifyAll();
    }
  }
}
