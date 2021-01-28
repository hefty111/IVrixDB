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

package org.apache.solr.ivrixdb.index.node.update;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;

/**
 * An abstraction for an object that sends Solr update commands to an IVrix bucket.
 * These Solr commands can be for indexing into, committing for, and
 * optimizing for the Solr indexes of an IVrix bucket.
 *
 * @author Ivri Faitelson
 */
public abstract class BucketUpdater {
  /**
   * @param bucketName The collection to update.
   * @return A local or remote collection updater. Chosen based on Solr's internal mechanisms for load balancing.
   */
  // TODO: add LocalBucketUpdater back in once it is fixed
  public static BucketUpdater createUpdater(String bucketName) {
    return new RemoteBucketUpdater(bucketName, IVrixLocalNode.getBaseURL());
  }

  /**
   * Adds an event to the bucket.
   * @param event The event to add.
   * @throws Exception If the event failed to index.
   */
  public abstract void addEvent(SolrInputDocument event) throws Exception;

  /**
   * Commits all changes in the bucket.
   * @throws Exception If failed to commit.
   */
  public abstract void commitChanges() throws Exception;

  /**
   * Waits until all update commands that were sent have finished processing.
   * @throws Exception If the updater can't finish properly.
   */
  public abstract void finish() throws Exception;

  /**
   * Closes the bucket updater.
   * @throws Exception If the updater can't close properly.
   */
  public abstract void close() throws Exception;
}
