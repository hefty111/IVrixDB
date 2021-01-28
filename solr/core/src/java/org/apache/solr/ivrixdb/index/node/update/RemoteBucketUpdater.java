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

import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * This objects updates a bucket by using Solr's own HTTP update client.
 *
 * @author Ivri Faitelson
 */
public class RemoteBucketUpdater extends BucketUpdater {
  private final ConcurrentUpdateSolrClient remoteCollectionClient;
  private final String bucketName;

  /**
   * @param bucketName The name of the bucket to update
   * @param nodeURL The URL of the Node to index into
   */
  RemoteBucketUpdater(String bucketName, String nodeURL) {
    this.remoteCollectionClient = new ConcurrentUpdateSolrClient.Builder(nodeURL).build();
    this.bucketName = bucketName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addEvent(SolrInputDocument event) throws Exception {
    remoteCollectionClient.add(bucketName, event);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void commitChanges() throws Exception {
    remoteCollectionClient.commit(bucketName);
  }

  /**
   *{@inheritDoc}
   */
  public void finish() throws Exception {
    remoteCollectionClient.blockUntilFinished();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws Exception {
    remoteCollectionClient.close();
  }
}
