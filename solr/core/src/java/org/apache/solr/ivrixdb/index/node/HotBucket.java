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

package org.apache.solr.ivrixdb.index.node;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.TimeBounds;
import org.apache.solr.ivrixdb.core.overseer.request.*;
import org.apache.solr.ivrixdb.index.node.update.BucketUpdater;
import org.apache.solr.ivrixdb.utilities.EventTimestampHelper;

/**
 * This object encapsulates a HOT bucket for indexing.
 * It can index events into the bucket, update frequently
 * updated bucket metadata in-memory, persist those metadata,
 * roll the bucket to WARM, and handle multiple event batches at once.
 *
 * @author Ivri Faitelson
 */
public class HotBucket {
  private final BucketUpdater bucketUpdater;
  private final String bucketName;
  private final String indexName;
  private final TimeBounds timeBounds;
  private int eventCount;

  private boolean rolledToWarm;
  private volatile int activeBatchCount;

  private HotBucket(String indexName, String bucketName) {
    this.bucketUpdater = BucketUpdater.createUpdater(bucketName);
    this.bucketName = bucketName;
    this.indexName = indexName;
    this.timeBounds = new TimeBounds(true);
    this.eventCount = 0;

    this.rolledToWarm = false;
    this.activeBatchCount = 0;
  }

  /**
   * Creates a new HOT bucket through the IVrix Overseer
   * @param indexName The name of the index that the new bucket will belong to
   * @param replicationFactor The number of replicas the bucket will have
   * @return A newly created HOT bucket
   * @throws Exception If failed to create the new bucket
   */
  public static HotBucket create(String indexName, int replicationFactor) throws Exception {
    IVrixOverseerOperationParams createBucketOperation = IVrixOverseerOperationParams.CreateBucket(
        indexName, replicationFactor
    );
    String bucketName = (String) IVrixOverseerClient.execute(createBucketOperation);
    return new HotBucket(indexName, bucketName);
  }

  /**
   * Indexes an event through Solr's default indexing mechanisms
   */
  public void addEvent(SolrInputDocument event) throws Exception {
    bucketUpdater.addEvent(EventTimestampHelper.createModifiedEventWithUnixTimestamp(event));
    timeBounds.updateBounds(EventTimestampHelper.getDateTime(event));
    eventCount += 1;
  }

  /**
   * Persists the cached metadata that can be persisted through the IVrix Overseer
   */
  public void updatePersistentMetadata() throws Exception {
    IVrixOverseerOperationParams operationParams = IVrixOverseerOperationParams.UpdateBucketMetadata(indexName, bucketName, timeBounds);
    IVrixOverseerClient.execute(operationParams);
  }

  /**
   * Commits all indexed events through Solr's default committing mechanisms
   */
  public void commitAllChanges() throws Exception {
    bucketUpdater.commitChanges();
  }


  /**
   * Increments the number of batches active. This is done in order to
   * prevent the bucket from closing while batches are still being indexed.
   */
  public void incrementActiveBatchCount() {
    synchronized (this) {
      activeBatchCount += 1;
    }
  }

  /**
   * Decrements the number of batches active. Closes the bucket if there are no
   * more active batches left.
   */
  public void decrementActiveBatchCount() throws Exception {
    synchronized (this) {
      activeBatchCount -= 1;
      closeUpdaterIfNecessary();
    }
  }

  /**
   * Rolls the bucket to WARM through the IVrix Overseer.
   * Closes the bucket if there are no more active batches left.
   */
  public void rollToWarm() throws Exception {
    this.rolledToWarm = true;
    IVrixOverseerOperationParams rollOperation = IVrixOverseerOperationParams.RollHotBucketToWarm(indexName, bucketName);
    IVrixOverseerClient.execute(rollOperation);
    closeUpdaterIfNecessary();
  }

  private void closeUpdaterIfNecessary() throws Exception {
    if (activeBatchCount == 0 && rolledToWarm) {
      bucketUpdater.finish();
      bucketUpdater.close();
    }
  }


  /**
   * @return The number of events in the bucket (cached value)
   */
  public int getSize() {
    return eventCount;
  }
}
