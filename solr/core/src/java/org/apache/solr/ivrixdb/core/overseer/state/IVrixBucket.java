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
import java.util.concurrent.CountDownLatch;

import org.apache.solr.common.cloud.*;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.*;
import org.slf4j.Logger;

import static org.apache.solr.ivrixdb.core.overseer.utilities.IVrixStateZkUtil.*;

/**
 * This object represents an IVrix Bucket. An IVrix Bucket is a logical
 * fragment of an IVrix Index. It possesses a timestamp range, an age state
 * (HOT, WARM, or COLD) and an attachment state (ATTACHED or DETACHED).
 * This object is responsible for creating, deleting, and manipulating
 * a bucket through the states listed above.
 *
 * HOT buckets are replicated across the cluster are being actively written into.
 * WARM buckets are replicated across the cluster that cannot be written into.
 * COLD buckets are local and cannot be written into. For this state, a new feature
 * was created so that COLD buckets can be detached/re-attached into Solr for resource management purposes.
 *
 * Even though buckets can be spread across the cluster, they still belong to the node that initially
 * had the leader replica on it. This allows each IVrix Node to manage its own IVrix Buckets.
 *
 * Currently, an IVrix Bucket is created as a Solr Collection with only one shard, a leader replica
 * that resides in the node that the Bucket belongs to, and replication replicas across the cluster
 * according to a provided replication factor.
 *
 * (IVrixBucket implements the fault-tolerant principles expressed in IVrixOverseer
 * {@link IVrixOverseer})
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: BUG FIX + FEATURE -- improve the deletion of an IVrix Bucket. The directories of detached buckets
 *                            are not being removed, and there is a bug that buckets that were indexed into
 *                            before a restart cannot be properly deleted and the system hangs...
 *
 * TODO: create an explicit state where the IVrixBucket is inactive because it does NOT have a TimeBounds,
 *       so that it doesnt participate at search and index rollover. There is also the case that Bucket does
 *       not possess Solr metadata. Implement bucket inactivity state to avoid confusion.
 *
 * TODO: the javadoc and the name in the function "isDetachedOnAllRelevantLiveNodes" DO NOT align with the code!
 *       Must correct this! The reason why it's not a bother is because the detach function in BucketsPhysicalHandler
 *       does not detach replicas from dead nodes
 *
 * TODO: in order to remove any confusion, signal [attach/detach]-in-process/finished at attach() and detach()
 *
 * TODO: refactoring idea -- re-implement and ensure that all solr state reading functions are clear and clean
 *                           (getResidingNodes, getCurrentlyResidingNodes, isAttachedOnAllRelevantLiveNodes, etc)
 *
 * TODO: refactoring idea -- clean up the whole attach/detach-in-state-only, and attach/detach-physically-only.
 *                           its kinda dirty...
 *
 * TODO: refactor idea -- rename updateMetadata() function name to "updateTimeBounds". Change all function names and Overseer
 *                        operations that use this function accordingly.
 *
 * TODO: refactor idea -- rename BucketType to BucketAge or BucketAgeState
 *
 * TODO: refactor idea -- create an AttachmentState enum for isDetached boolean
 *
 * TODO: refactor idea -- change isAttached() and isDetached() to "is[Attached/Detached]InState" or what not
 */
public class IVrixBucket {
  private static final Logger log = IVrixOverseer.log;

  /**
   * Defines an age state of an IVrix Bucket.
   * Can be either HOT, WARM, or COLD.
   */
  // ORDER MATTERS! The ordinal values are being utilized
  public enum BucketType {
    HOT, WARM, COLD
  }

  private final String indexName;
  private final String bucketName;
  private final String indexerNodeName;
  private SolrCollectionMetadata solrMetadata;
  private TimeBounds timeBounds;
  private BucketType bucketType;
  private Boolean isDetached;

  private final List<BucketHolder> currentHolders;
  private CountDownLatch attachCompletionLatch;
  private CountDownLatch detachCompletionLatch;

  private IVrixBucket(String indexName, String bucketName, String indexerNodeName,
                      SolrCollectionMetadata solrCollectionMetadata,
                      TimeBounds timeBounds, BucketType bucketType, boolean isDetached) {
    this.indexName = indexName;
    this.bucketName = bucketName;
    this.indexerNodeName = indexerNodeName;
    this.timeBounds = timeBounds;
    this.solrMetadata = solrCollectionMetadata;
    this.isDetached = isDetached;
    this.bucketType = bucketType;
    this.currentHolders = new LinkedList<>();
    this.attachCompletionLatch = new CountDownLatch(0);
    this.detachCompletionLatch = new CountDownLatch(0);
  }

  /**
   * Creates a new, default, and empty IVrix Bucket without persistence or physical existence
   * That is, it does not exist in IVrixDB's state, nor in Solr's state.
   *
   * @param indexName The name of the IVrix Index that this Bucket belongs to
   * @param bucketName The name to give the Bucket
   * @param indexerNodeName The name of the Node that this Bucket belongs to
   */
  public IVrixBucket(String indexName, String bucketName, String indexerNodeName) {
    this(indexName, bucketName, indexerNodeName,
        null, null,
        BucketType.HOT, false
    );
  }

  /**
   * Creates existence for self in persistence and physically (that is, in IVrixDB's state and in Solr's state)
   */
  public void createSelf(List<String> nodesForReplication, int replicationFactor) throws IOException {
    log.info("creating bucket " + bucketName + " for index " + indexName + "...");

    createBucketBlobNode(this);
    createIsDetachedPropertyNode(this);
    createBucketTypePropertyNode(this);

    IVrixLocalNode.getBucketsPhysicalHandler().createBucket(
        bucketName, indexerNodeName, nodesForReplication, replicationFactor
    );
    this.updateSolrMetadata();
  }

  /**
   * @return An IVrix Bucket with the requested name that was persisted in the requested Index name.
   *          Loads the persistent properties of the Bucket from Zookeeper, and has
   *          non-persistent properties at default values.
   */
  public static IVrixBucket loadFromZK(String indexName, String bucketName) throws IOException {
    log.info("loading bucket " + bucketName + " for index " + indexName + "...");

    boolean isDetached = retrieveIsDetachedPropertyValue(indexName, bucketName);
    BucketType bucketType = retrieveBucketTypePropertyValue(indexName, bucketName);
    Map<String, Object> metadataBlob = retrieveBucketBlob(indexName, bucketName);
    Map<String, Object> solrMetadataMap = (Map<String, Object>)metadataBlob.get(SOLR_COLLECTION_METADATA_KEY);
    Map<String, Object> timeBoundsMap = (Map<String, Object>)metadataBlob.get(TIME_BOUNDS_KEY);
    String indexerNodeName = (String)metadataBlob.get(INDEXER_NODE_NAME_KEY);

    return new IVrixBucket(
        indexName,
        bucketName,
        indexerNodeName,
        solrMetadataMap == null ? null : SolrCollectionMetadata.fromMap(bucketName, solrMetadataMap),
        timeBoundsMap == null ? null : TimeBounds.fromMap(timeBoundsMap),
        bucketType,
        isDetached
    );
  }

  /**
   * Deletes an IVrix Bucket from persistence and physically (that is, from IVrixDB's state and from Solr's state)
   */
  public static void delete(IVrixBucket bucket) throws IOException {
    log.info("deleting bucket " + bucket.getName() + " from index " + bucket.getIndexName() + "...");

    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollectionOrNull(bucket.getName());
    if (collectionState != null) {
      IVrixLocalNode.getBucketsPhysicalHandler().deleteBucket(bucket.getName());
    }
    deleteIVrixBucketPath(bucket);
  }

  /**
   * Rolls the Bucket to WARM
   */
  public void rollToWarm() throws IOException {
    log.info("rolling bucket " + bucketName + " to warm...");
    if (bucketType.ordinal() > BucketType.WARM.ordinal()) {
      throw new IllegalStateException("cannot roll " + bucketName + " to WARM, since it's state is " + bucketType);
    }

    setBucketType(BucketType.WARM);
  }

  /**
   * Rolls the Bucket to COLD. Deletes all replication replicas from Bucket (in both IVrixDB and Solr states).
   */
  public void rollToCold() throws IOException {
    log.info("rolling bucket " + bucketName + " to cold...");
    if (bucketType.ordinal() > BucketType.COLD.ordinal()) {
      throw new IllegalStateException("cannot roll " + bucketName + " to COLD, since it's state is " + bucketType);
    }

    setBucketType(BucketType.COLD);
    IVrixLocalNode.getBucketsPhysicalHandler().prepareWarmBucketForColdRollover(this);
    updateSolrMetadata();
  }

  /**
   * Attaches the Bucket, both in IVrixDB state and in Solr's state (i.e, in state and physically)
   */
  public void attach() throws IOException {
    attach(true);
  }

  /**
   * Detaches the Bucket, both in IVrixDB state and in Solr's state (i.e, in state and physically)
   */
  public void detach() throws IOException {
    detach(true);
  }

  /**
   * Attaches the Bucket, in IVrixDB state only (i.e, in state only).
   * Used for releasing global state hold before attach/detach finishes.
   * For other cases, remember to attach Bucket physically afterwards.
   */
  public void attachInStateOnly() throws IOException {
    attach(false);
  }

  /**
   * Detaches the Bucket, in IVrix state only (i.e, in state only).
   * Used for releasing global state hold before attach/detach finishes.
   * For other cases, remember to detach Bucket physically afterwards.
   */
  public void detachInStateOnly() throws IOException {
    detach(false);
  }

  /**
   * Attaches the Bucket, in Solr state only (i.e, physically only).
   * Used for releasing global state hold before attach/detach finishes.
   * For other cases, remember to attach Bucket in state first.
   */
  public void physicallyAttachOnly() {
    IVrixLocalNode.getBucketsPhysicalHandler().attachBucket(this);
  }

  /**
   * Detaches the Bucket, in Solr state only (i.e, physically only).
   * Used for releasing global state hold before attach/detach finishes.
   * For other cases, remember to detach Bucket in state first.
   */
  public void physicallyDetachOnly() {
    IVrixLocalNode.getBucketsPhysicalHandler().detachBucket(this);
  }

  private void attach(boolean toPhysicallyAttach) throws IOException {
    log.info("attaching bucket " + bucketName + "...");
    if (isAttachedOnAllRelevantLiveNodes()) {
      throw new IllegalStateException("cannot attach " + bucketName + ", since it is already attached in all relevant live nodes");
    }

    setToAttached();
    if (toPhysicallyAttach) {
      physicallyAttachOnly();
    }
  }

  private void detach(boolean toPhysicallyDetach) throws IOException {
    log.info("detaching bucket " + bucketName + "...");
    if (isBeingHeld()) {
      throw new IllegalStateException("cannot detach " + bucketName + ", since it is currently being held");

    } else if (isDetachedOnAllRelevantLiveNodes()) {
      throw new IllegalStateException("cannot detach " + bucketName + ", since it is is already detached in all relevant live nodes");
    }

    setToDetached();
    if (toPhysicallyDetach) {
      physicallyDetachOnly();
    }
  }

  private void updateSolrMetadata() throws IOException {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollection(bucketName);
    this.solrMetadata = SolrCollectionMetadata.copy(collectionState);
    updateBucketBlobNode(this);
  }

  private void setToAttached() throws IOException {
    this.isDetached = false;
    updateIsDetachedPropertyNode(this);
  }

  private void setToDetached() throws IOException {
    this.isDetached = true;
    updateIsDetachedPropertyNode(this);
  }

  private void setBucketType(BucketType bucketType) throws IOException {
    this.bucketType = bucketType;
    updateBucketTypePropertyNode(this);
  }




  /**
   * Updates the time bounds of the Bucket in IVrixDB's state. Used during indexing.
   */
  public void updateMetadata(TimeBounds timeBounds) throws IOException {
    this.timeBounds = timeBounds;
    updateBucketBlobNode(this);
  }

  /**
   * Adds a holder to the list of holders that are currently searching the bucket. Used for searching.
   */
  public void addHolder(String searchJobID, String requestingNodeName) {
    currentHolders.add(new BucketHolder(searchJobID, requestingNodeName));
  }

  /**
   * removes a holder from the list of holders
   */
  public void removeHolder(String searchJobID, String requestingNodeName) {
    BucketHolder holderToRemove = new BucketHolder(searchJobID, requestingNodeName);
    currentHolders.removeIf(nextHolder -> nextHolder.equals(holderToRemove));
  }

  /**
   * Removes all holders that were created by a specific IVrix Node from the list of hodlers
   */
  public void removeAllHoldersFromNode(String nodeBaseURL) {
    currentHolders.removeIf(nextHolder -> nextHolder.nodeName.equals(nodeBaseURL));
  }

  /**
   * Forcibly removes all holders from the list of holders
   */
  public void forceRemoveAllHolders() {
    currentHolders.clear();
  }

  /**
   * Marks that physical attachment is in process. USED ONLY AFTER attachment was marked in IVrixDB state.
   * Used for ensuring that operations that use attach/detach will wait for physical state to finish changing.
   */
  public void signalAttachInProcess() {
    attachCompletionLatch = new CountDownLatch(1);
  }

  /**
   * Marks that physical detachment is in process. USED ONLY AFTER attachment was marked in IVrixDB state.
   * Used for ensuring that operations that use attach/detach will wait for physical state to finish changing.
   */
  public void signalDetachInProcess() {
    detachCompletionLatch = new CountDownLatch(1);
  }

  /**
   * Marks that physical attachment has completed
   */
  public void signalAttachIsFinished() {
    attachCompletionLatch.countDown();
  }

  /**
   * Marks that physical detachment has completed
   */
  public void signalDetachIsFinished() {
    detachCompletionLatch.countDown();
  }

  /**
   * Thread/operation will wait until physical attachment of the Bucket has finished
   */
  public void waitUntilAttachIsFinished() throws IOException {
    try {
      attachCompletionLatch.await();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Thread/operation will wait until physical detachment of the Bucket has finished
   */
  public void waitUntilDetachIsFinished() throws IOException {
    try {
      detachCompletionLatch.await();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }


  /**
   * @return The name of the IVrix Index that this Bucket belongs to
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * @return The name of this Bucket
   */
  public String getName() {
    return bucketName;
  }

  /**
   * @return The name of the IVrix Node that this Bucket belongs to
   */
  public String getIndexerNodeName() {
    return indexerNodeName;
  }

  /**
   * @return The Bucket's timestamp range
   */
  // WARNING: TimeBounds MAY BE null
  public TimeBounds getTimeBounds() {
    return timeBounds;
  }

  /**
   * @return A single number representation of the Bucket's timestamp range
   */
  public long getLongRepresentationOfTimeBounds() {
    return timeBounds.getLongRepresentation();
  }

  /**
   * @return The age state of the Bucket
   */
  public BucketType getBucketType() {
    return bucketType;
  }

  /**
   * @return The objects in the Solr state that correspond to the Bucket
   */
  public SolrCollectionMetadata getSolrMetadata() {
    return solrMetadata;
  }

  /**
   * @return A Map blob containing the main persisted metadata of the Bucket
   */
  public Map<String, Object> getMainMetadataBlob() {
    Map<String, Object> metadataMap = new HashMap<>();
    metadataMap.put(INDEXER_NODE_NAME_KEY, indexerNodeName);
    metadataMap.put(SOLR_COLLECTION_METADATA_KEY, solrMetadata == null ? null : solrMetadata.toMap());
    metadataMap.put(TIME_BOUNDS_KEY, timeBounds == null ? null : timeBounds.toMap());
    return metadataMap;
  }

  /**
   * @return All the IVrix Nodes where the Bucket resides in, disregarding detached and dead replicas
   */
  public List<String> getResidingNodes() {
    return getResidingNodes(solrMetadata.getSlices());
  }

  /**
   * @return All the IVrix Nodes where the Bucket currently sits at, excluding detached and dead replicas
   */
  public List<String> getCurrentlyResidingNodes() {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollectionOrNull(bucketName);
    if (collectionState == null) {
      return new LinkedList<>();
    }
    return getResidingNodes(collectionState.getSlices());
  }

  private List<String> getResidingNodes(Collection<Slice> shards) {
    List<String> currentlyResidingNodes = new LinkedList<>();
    for (Slice shard : shards) {
      for (Replica replica : shard.getReplicas()) {
        String residingNodeName = replica.getNodeName();
        if (!currentlyResidingNodes.contains(residingNodeName)) {
          currentlyResidingNodes.add(residingNodeName);
        }
      }
    }
    return currentlyResidingNodes;
  }




  /**
   * @return Whether the bucket is being held by a holder
   */
  public boolean isBeingHeldByHolder(String searchJobID, String requestingNodeName) {
    BucketHolder holder = new BucketHolder(searchJobID, requestingNodeName);
    return currentHolders.contains(holder);
  }

  /**
   * @return Whether the bucket is being held at all
   */
  public boolean isBeingHeld() {
    return currentHolders.size() > 0;
  }

  /**
   * @return Whether the Bucket is being attached and hasn't finished yet
   */
  public boolean isAttachInProcess() {
    return attachCompletionLatch.getCount() > 0;
  }

  /**
   * @return Whether the Bucket is being detached and hasn't finished yet
   */
  public boolean isDetachInProcess() {
    return detachCompletionLatch.getCount() > 0;
  }

  /**
   * @return Whether the Bucket is marked as attached in IVrix state
   */
  public boolean isAttached() {
    return !isDetached;
  }

  /**
   * @return Whether the Bucket is marked as detached in IVrix state
   */
  public boolean isDetached() {
    return isDetached;
  }

  /**
   * @return Whether the Bucket is at age state HOT
   */
  public boolean isHot() {
    return bucketType == BucketType.HOT;
  }

  /**
   * @return Whether the Bucket is at age state WARM
   */
  public boolean isWarm() {
    return bucketType == BucketType.WARM;
  }

  /**
   * @return Whether the Bucket is at age state COLD
   */
  public boolean isCold() {
    return bucketType == BucketType.COLD;
  }

  /**
   * @return Whether the Bucket has stored the objects in the Solr state that correspond to the Bucket
   */
  public boolean isSolrMetadataNotFilled() {
    return solrMetadata == null;
  }

  /**
   * @return Whether the Bucket is attached on the Nodes that it is
   *          supposed to be attached at that are live
   */
  public boolean isAttachedOnAllRelevantLiveNodes() {
    Collection<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    boolean isAttachedOnAllRelevantLiveNodes = true;

    for (Slice shardMetadata : solrMetadata.getSlices()) {
      for (Replica replicaMetadata : shardMetadata.getReplicas()) {
        boolean isReplicaAttached = SolrCollectionMetadata.isReplicaActuallyAttached(bucketName, shardMetadata.getName(), replicaMetadata.getCoreName());
        boolean isReplicaOnLiveNode = liveNodes.contains(replicaMetadata.getNodeName());

        if (!isReplicaAttached && isReplicaOnLiveNode) {
          isAttachedOnAllRelevantLiveNodes = false;
        }
      }
    }
    return isAttachedOnAllRelevantLiveNodes;
  }

  /**
   * @return Whether the Bucket is detached on the Nodes that it is
   *          supposed to be detached at that are live
   */
  public boolean isDetachedOnAllRelevantLiveNodes() {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollection(bucketName);
    boolean isDetachedOnAllRelevantLiveNodes = true;
    Collection<Slice> shards = collectionState.getSlices();
    for (Slice shard : shards) {
      if (shard.getReplicas().size() > 0) {
        isDetachedOnAllRelevantLiveNodes = false;
      }
    }
    return isDetachedOnAllRelevantLiveNodes;
  }

  /**
   * @return Whether the Bucket is attached with at least one replica
   *          that is live and healthy enough to be queried
   */
  public boolean isAttachedWithAtLeastOneQueryableReplica() {
    ClusterState clusterState = IVrixLocalNode.getClusterState();
    DocCollection collectionState = clusterState.getCollection(bucketName);
    Collection<String> liveSolrNodes = clusterState.getLiveNodes();

    boolean isAttachedWithAtLeastOneActiveReplica = false;
    for (Replica replica : collectionState.getReplicas()) {
      if (replica.getState() == Replica.State.ACTIVE && liveSolrNodes.contains(replica.getNodeName())) {
        isAttachedWithAtLeastOneActiveReplica = true;
        break;
      }
    }
    return isAttachedWithAtLeastOneActiveReplica;
  }

  /**
   * @return Whether the Bucket can be attached with at least one replica
   *          that can be live and healthy enough to be queried
   */
  public boolean canAttachWithAtLeastOneQueryableReplica() {
    Collection<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    boolean canAttachWithAtLeastOneQueryableReplica = false;

    for (Replica replicaMetadata : solrMetadata.getReplicas()) {
      if (liveNodes.contains(replicaMetadata.getNodeName())) {
        canAttachWithAtLeastOneQueryableReplica = true;
        break;
      }
    }
    return canAttachWithAtLeastOneQueryableReplica;
  }
}
