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

import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.TimeBounds;
import org.apache.solr.ivrixdb.core.overseer.utilities.IVrixStateZkUtil;

/**
 * This object represents an IVrix Index. An IVrix index is a schema-less and distributed
 * time-series index with data aging capabilities. It is separated into buckets,
 * where each bucket is a slice of the index. The schema of the Index is comprised of
 * two fields: a raw log, and the extracted timestamp. Each bucket is the equivalent to
 * a Solr shard, where its age rolls through the states of HOT, WARM, and COLD.
 * This object is responsible for creating/deleting the Index and for retrieving Buckets.
 *
 * As an IVrix index grows, and reaches certain criteria, it will perform bucket rollovers,
 * where the oldest buckets in each age state are rolled to the next state. This occurs when
 * the IVrix Overseer executes a CREATE BUCKET operation.
 *
 * To learn more about IVrix Buckets, please refer to {@link IVrixBucket}.
 *
 * (IVrixIndex implements the fault-tolerant principles expressed in IVrixOverseer {@link IVrixOverseer})
 *
 * @author Ivri Faitelson
 */
public class IVrixIndex {
  private final Map<String, IVrixBucket> bucketMap;
  private final String indexName;

  private IVrixIndex(String indexName) {
    this.bucketMap = new HashMap<>();
    this.indexName = indexName;
  }

  /**
   * @param indexName The name for an Index
   * @return The created empty IVrix Index
   */
  public static IVrixIndex create(String indexName) throws IOException {
    IVrixStateZkUtil.createIVrixIndexPath(indexName);
    return new IVrixIndex(indexName);
  }

  /**
   * Deletes an IVrix Index, given its name
   */
  public static void delete(String indexName) throws IOException {
    IVrixStateZkUtil.deleteIVrixIndexPath(indexName);
  }

  /**
   * @return Loads the persistent properties of an IVrix Index and its respective Buckets from Zookeeper,
   *          and has non-persistent properties at default values
   */
  public static IVrixIndex loadFromZK(String indexName) throws IOException {
    IVrixIndex index = new IVrixIndex(indexName);
    List<String> bucketNames = IVrixStateZkUtil.retrieveAllPersistedBucketNames(indexName);

    for (String bucketName : bucketNames) {
      IVrixBucket loadedBucket = IVrixBucket.loadFromZK(indexName, bucketName);
      index.addBucket(loadedBucket);
    }
    return index;
  }

  /**
   * Adds an IVrix Bucket to this IVrix Index
   */
  public void addBucket(IVrixBucket bucket) {
    bucketMap.put(bucket.getName(), bucket);
  }

  /**
   * @return The IVrix Buckets that overlap with the given TimeBounds (for search)
   */
  public List<IVrixBucket> getBucketsOverlappingWithTimeBounds(TimeBounds timeBounds) {
    List<IVrixBucket> buckets = new LinkedList<>();
    for (IVrixBucket bucket : bucketMap.values()) {
      if (bucket.getTimeBounds() != null && bucket.getTimeBounds().overlapsWith(timeBounds)) {
        buckets.add(bucket);
      }
    }
    return buckets;
  }

  /**
   * @return The IVrix Bucket with the requested name
   */
  public IVrixBucket getBucket(String bucketName) {
    return bucketMap.get(bucketName);
  }

  /**
   * @return All the IVrix Buckets in this IVrix Index
   */
  public Collection<IVrixBucket> getBuckets() {
    return bucketMap.values();
  }

  /**
   * @return The name of the next future IVrix Bucket for this IVrix Index.
   *          The name is the latest bucket number plus 1.
   *          This is done due to potential bucket deletion issues.
   */
  public String nextNewBucketName() {
    return indexName + (numberOfLatestBucket() + 1);
  }

  private int numberOfLatestBucket() {
    int latestBucketNumber = 0;
    for (String bucketName : bucketMap.keySet()) {
      int bucketNumber = Integer.parseInt(bucketName.replace(indexName, ""));
      if (bucketNumber > latestBucketNumber) {
        latestBucketNumber = bucketNumber;
      }
    }
    return latestBucketNumber;
  }
}
