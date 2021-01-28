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

package org.apache.solr.ivrixdb.core.overseer;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.solr.ivrixdb.core.overseer.request.IVrixOverseerClient;
import org.apache.solr.ivrixdb.core.overseer.request.IVrixOverseerOperationParams;
import org.apache.solr.ivrixdb.core.overseer.state.*;
import org.apache.solr.ivrixdb.core.overseer.utilities.*;
import org.apache.solr.ivrixdb.core.overseer.election.LeaderElectionCandidate;
import org.apache.solr.ivrixdb.core.overseer.response.OrganizedBucketsForSearch;
import org.slf4j.*;

/**
 * The IVrix Overseer is responsible for managing the IVrixDB global state,
 * the IVrix Indexes and their respective IVrix Buckets. It is a thread-safe,
 * fault-tolerant, leader-elected (and re-elected) role that any IVrix Node can take on.
 * The election works through Zookeeper and is a "first-come, first-serve" election.
 * Each request to the Overseer is a new thread, and so the Overseer is designed to be a thread-safe object.
 * The IVrix Overseer sends commands over to Solr’s Overseer to change Solr’s state,
 * thereby allowing Solr to manage its own state undisturbed.
 *
 * A small portion of the Overseer’s state is held in-memory and will die with the Overseer,
 * and the rest is persisted into Zookeeper, which is loaded and cached during Overseer boot-up.
 *
 * The Overseer also manages the overall bucket attachment count across all
 * IVrix Indexes at each IVrix Node. It does so at each bucket creation by executing
 * index rollover commands if necessary, and at each bucket hold request (for search)
 * by executing attach/detach commands if necessary.
 *
 * The Overseer implements fault-tolerance by enabling its operations to work around dead nodes
 * and by ensuring consistency and recovery from non-completed operations at three major checkpoints:
 * Overseer boot-up, Overseer handling of Node boot-up and Overseer handling of Node shutdown.
 * It ensures that it can stay consistent and recover at those checkpoints because each Solr-state-changing
 * operation first marks the change in IVrixDB's state at Zookeeper before enacting the operation.
 * The ONLY exception are deletion commands, which first delete from Solr state before
 * deleting from IVrixDB state.
 *
 * Since the Overseer is a leader-elected object and there are dormant Overseer's waiting to be elected as leader
 * for every IVrix Node, requests are sent to the Overseer through the IVrix Overseer Client, which finds the elected
 * leader and sends the request to it. To learn more about the client, please refer to {@link IVrixOverseerClient}.
 *
 * To learn more about IVrix Indexes and IVrix Buckets, please refer to {@link IVrixBucket} and {@link IVrixIndex}.
 *
 * A description of each Operation that the IVrix Overseer can handle can be found at {@link IVrixOverseerOperationParams}.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: redo entire thread-safety mechanisms. It only allows one operation to execute at a time,
 *       when there are operations that can safe simultaneously.
 */
public class IVrixOverseer extends LeaderElectionCandidate {
  public static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CountDownLatch initLatch;
  private final IVrixState state;

  /**
   * Creates an IVrix Overseer candidate
   */
  public IVrixOverseer() {
    super();
    initLatch = new CountDownLatch(1);
    state = new IVrixState();
  }

  /**
   * Changes the mode of the IVrix Overseer candidate.
   * If the mode is "LEADER", then this candidate
   * is the Overseer and it would be booted up.
   */
  @Override
  protected void operateAs(OperatingMode operatingMode) throws Exception {
    super.operateAs(operatingMode);
    if (operatingMode == OperatingMode.LEADER) {
      log.info("Initializing Overseer...");
      state.loadFromZK();
      state.ensureStateConsistency();
      state.freeHotBucketsFromDownedNodes();
      log.info("Overseer is initialized.");
      initLatch.countDown();
    }
  }

  /**
   * Handles a change in the list of IVrix Overseer candidates (IVrix Nodes)
   * @param changeType The type of change in the list (either CREATED or DELETED)
   * @param slaveID The candidate ID that introduced the change in the list
   */
  @Override
  protected void handleSlaveChange(ChangeType changeType, String slaveID) {
    String slaveNodeName = CandidateIdUtil.extractNodeNameFromID(slaveID);
    try {
      state.acquireLock();
      if (changeType == ChangeType.CREATED) {
        log.info("Overseer is handling the addition of a slave...");
        state.ensureStateConsistency();
        log.info("Overseer finished handling the addition of a slave.");

      } else if (changeType == ChangeType.DELETED) {
        log.info("Overseer is handling the removal of a slave...");
        state.ensureStateConsistency();
        state.freeHotBucketsFromNode(slaveNodeName);
        state.removeAllHoldersFromNode(slaveNodeName);
        state.notifyUpdateWaiters();
        log.info("Overseer finished handling the removal of a slave.");
      }
      state.releaseLock();

    } catch (Exception e) {
      log.info("Overseer failed at handling the boot state change of slave node: " + slaveNodeName +
               ". Will the log the error, and resume tasks: ", e);
      e.printStackTrace();

    } finally {
      state.releaseLockIfAcquired();
    }
  }



  /**
   * Executes an operation against this IVrix Overseer candidate.
   * Waits for the this IVrix Overseer candidate to boot up before executing.
   * If it is not in mode "LEADER", then it cannot execute the operation.
   */
  public Object execute(IVrixOverseerOperationParams params) throws Exception {
    if (getOperatingMode() == OperatingMode.SLAVE || getOperatingMode() == OperatingMode.NOT_RUNNING) {
      throw new IllegalAccessException("Cannot execute against a slave or a not-running instance of overseer!");
    }
    initLatch.await();

    Object response = null;
    try {
      switch (params.getOperationType()) {
        case FREE_HOT_BUCKETS_IN_NODE:
          response = executeFreeHotBucketsInNodeOperation(params);
          break;

        case CREATE_IVRIX_INDEX:
          response = executeCreateIVrixIndex(params);
          break;
        case DELETE_IVRIX_INDEX:
          response = executeDeleteIVrixIndex(params);
          break;
        case GET_ALL_IVRIX_INDEXES:
          response = executeGetAllIVrixIndexes(params);
          break;

        case CREATE_BUCKET:
          response = executeCreateBucketOperation(params);
          break;
        case UPDATE_BUCKET_METADATA:
          response = executeUpdateBucketMetadataOperation(params);
          break;
        case ROLL_HOT_BUCKET_TO_WARM:
          response = executeRollHotBucketToWarmOperation(params);
          break;

        case GET_BUCKETS_WITHIN_BOUNDS:
          response = executeGetBucketsForSearchJobOperation(params);
          break;
        case HOLD_BUCKET_FOR_SEARCH:
          response = executeHoldBucketForSearchOperation(params);
          break;
        case RELEASE_BUCKET_HOLD:
          response = executeReleaseBucketHoldOperation(params);
          break;
      }
    } finally {
      state.releaseLockIfAcquired();
    }
    return response;
  }

  private Object executeFreeHotBucketsInNodeOperation(IVrixOverseerOperationParams params) throws Exception {
    String indexerNodeName = params.getRequestingNodeName();
    state.freeHotBucketsFromNode(indexerNodeName);
    return true;
  }



  private Object executeCreateIVrixIndex(IVrixOverseerOperationParams params) throws Exception {
    String indexName = params.getIndexName();
    state.createIVrixIndex(indexName);
    return true;
  }

  private Object executeDeleteIVrixIndex(IVrixOverseerOperationParams params) throws Exception {
    String indexName = params.getIndexName();
    state.deleteIVrixIndex(indexName);
    return true;
  }

  private Object executeGetAllIVrixIndexes(IVrixOverseerOperationParams params) {
    return state.getAllIndexNames();
  }





  private Object executeCreateBucketOperation(IVrixOverseerOperationParams params) throws Exception {
    String indexName = params.getIndexName();
    String indexerNodeName = params.getRequestingNodeName();
    int replicationFactor = params.getReplicationFactor();

    List<String> nodesForReplication = state.getNodesForBucketReplication(indexerNodeName, replicationFactor);
    List<String> allNodesForCreation = new LinkedList<>(nodesForReplication);
    allNodesForCreation.add(0, indexerNodeName);

    state.acquireLock();
    for (String nameOfNodeForCreation : allNodesForCreation) {
      state.makeRoomInNodeForNewBucketIfNecessary(nameOfNodeForCreation);
      state.ensureColdBucketsDoNotExceedLimitInNode(nameOfNodeForCreation);
    }
    String bucketName = state.getIndex(indexName).nextNewBucketName();
    state.createIVrixBucket(indexName, bucketName, indexerNodeName, nodesForReplication, replicationFactor);
    state.releaseLock();

    return bucketName;
  }

  private Object executeUpdateBucketMetadataOperation(IVrixOverseerOperationParams params) throws Exception {
    IVrixBucket bucket = state.getIndex(params.getIndexName()).getBucket(params.getBucketName());
    bucket.updateMetadata(params.getTimeBounds());
    return bucket;
  }

  private Object executeRollHotBucketToWarmOperation(IVrixOverseerOperationParams params) throws Exception {
    IVrixBucket bucket = state.getIndex(params.getIndexName()).getBucket(params.getBucketName());
    if (bucket.isHot()) {
      bucket.rollToWarm();
    } else {
      throw new IllegalStateException(
          "Cannot signal finished indexing into a non-hot bucket. Indexing should ONLY occur in HOT buckets"
      );
    }
    return true;
  }






  private Object executeGetBucketsForSearchJobOperation(IVrixOverseerOperationParams params) throws Exception {
    IVrixIndex index = state.getIndex(params.getIndexName());
    List<IVrixBucket> buckets = index.getBucketsOverlappingWithTimeBounds(params.getTimeBounds());
    return OrganizedBucketsForSearch.fromBucketsList(buckets);
  }

  private Object executeHoldBucketForSearchOperation(IVrixOverseerOperationParams params) throws Exception {
    IVrixBucket bucket = state.getIndex(params.getIndexName()).getBucket(params.getBucketName());
    boolean success = tryToHoldBucket(bucket, params);
    if (!success) {
      bucket.removeHolder(params.getSearchJobId(), params.getRequestingNodeName());
    }
    state.notifyUpdateWaiters();
    return success;
  }

  private Object executeReleaseBucketHoldOperation(IVrixOverseerOperationParams params) throws Exception {
    IVrixBucket bucket = state.getIndex(params.getIndexName()).getBucket(params.getBucketName());
    bucket.removeHolder(params.getSearchJobId(), params.getRequestingNodeName());
    state.notifyUpdateWaiters();
    return true;
  }




  private boolean tryToHoldBucket(IVrixBucket bucketToAttach, IVrixOverseerOperationParams params) throws Exception {
    state.acquireLock();
    addHolderToListIfNonExistent(bucketToAttach, params);

    if (bucketToAttach.isAttached()) {
      state.releaseLock();
      if (bucketToAttach.isAttachInProcess()) {
        bucketToAttach.waitUntilAttachIsFinished();
      }

    } else {
      if (bucketToAttach.isDetachInProcess()) {
        bucketToAttach.waitUntilDetachIsFinished();
      }
      tryToAttachBucket(bucketToAttach, params);
    }
    return bucketToAttach.isAttachedWithAtLeastOneQueryableReplica();
  }

  private void tryToAttachBucket(IVrixBucket bucketToAttach, IVrixOverseerOperationParams params) throws Exception {
    if (bucketToAttach.canAttachWithAtLeastOneQueryableReplica()) {
      Set<String> residingNodesThatReachedMaxAttachedCold = state.getResidingNodesThatReachedMaxAttachedCold(bucketToAttach);
      Set<IVrixBucket> bucketsToDetach = state.getBucketsToDetach(residingNodesThatReachedMaxAttachedCold);
      boolean canMakeEnoughRoomForAttachment = residingNodesThatReachedMaxAttachedCold.size() == bucketsToDetach.size();

      if (canMakeEnoughRoomForAttachment) {
        state.setStatesToDetachAndAttach(bucketsToDetach, bucketToAttach);
        state.releaseLock();
        state.physicallyDetachAndAttach(bucketsToDetach, bucketToAttach);

      } else {
        state.releaseLock();
        state.waitForUpdate(bucketToAttach);
        tryToHoldBucket(bucketToAttach, params);
      }
    } else {
      state.releaseLock();
    }
  }

  private void addHolderToListIfNonExistent(IVrixBucket bucketToAttach, IVrixOverseerOperationParams params) throws Exception {
    if (!bucketToAttach.isBeingHeldByHolder(params.getSearchJobId(), params.getRequestingNodeName())) {
      bucketToAttach.addHolder(params.getSearchJobId(), params.getRequestingNodeName());
    }
  }
}
