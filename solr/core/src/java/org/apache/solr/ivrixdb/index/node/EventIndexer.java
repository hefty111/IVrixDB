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

import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.utilities.Constants;

/**
 * This object indexes an events batch into an IVrix Index.
 * It indexes into buckets that belong to the local IVrix Node.
 * It uses Solr's internal indexing mechanisms to index the batch.
 *
 * @author Ivri Faitelson
 */
public class EventIndexer {
  private static final ReentrantLock batchLock = new ReentrantLock();

  private final String indexName;
  private HotBucket hotBucket;

  /**
   * @param indexName The IVrix index to index events into
   */
  public EventIndexer(String indexName) {
    this.indexName = indexName;
  }

  /**
   * Prepares for the batch by selecting a HOT bucket for indexing.
   * If no HOT buckets exist, it will make one.
   * If all HOT buckets are full, it will roll a HOT bucket and make a new one.
   */
  public void prepareForBatch() throws Exception {
    batchLock.lock();
    hotBucket = IVrixLocalNode.getHotBucket(indexName);

    if (hotBucket == null) {
      createAndStoreBucket();

    } else if (hotBucket.getSize() >= Constants.IVrix.DEFAULT_MAX_EVENT_COUNT_IN_BUCKET) {
      hotBucket.rollToWarm();
      IVrixLocalNode.removeHotBucket(indexName);
      createAndStoreBucket();
    }

    hotBucket.incrementActiveBatchCount();
    batchLock.unlock();
  }

  /**
   * Indexes an event from the events batch into a HOT bucket of the IVrix Index
   */
  public void index(SolrInputDocument event) throws Exception {
    hotBucket.addEvent(event);
  }

  /**
   * Finishes indexing the batch. Commits all events in the HOT bucket.
   * Updates the bucket metadata (time-bounds for example) into persistent storage.
   */
  public void close() throws Exception {
    hotBucket.updatePersistentMetadata();
    hotBucket.commitAllChanges();
    hotBucket.decrementActiveBatchCount();
  }


  private void createAndStoreBucket() throws Exception {
    hotBucket = HotBucket.create(indexName, Constants.IVrix.DEFAULT_BUCKET_REPLICATION_FACTOR);
    IVrixLocalNode.storeHotBucket(indexName, hotBucket);
  }
}
