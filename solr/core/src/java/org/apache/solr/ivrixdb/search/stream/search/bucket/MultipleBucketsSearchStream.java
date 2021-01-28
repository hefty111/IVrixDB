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

package org.apache.solr.ivrixdb.search.stream.search.bucket;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.ivrixdb.utilities.SolrObjectMocker;


/**
 * This stream searches multiple buckets at once (a batch, essentially.)
 * It inherits the stream sorting/reading functionality from CloudSolrStream,
 * so that reverse time order is ensured at the level of a batch.
 *
 * @author Ivri Faitelson
 */
/*
 * NOTE: If a stream wasn't read within 2 minutes, it will timeout.
 *       All streams will throw a CRLF error on the next read() call.
 */
public class MultipleBucketsSearchStream extends CloudSolrStream {
  private final List<BucketSearchStreamWrapper> bucketSearchStreamWrappers;
  /**
   * @param bucketNames The names of the buckets in the batch
   * @param zkHost The zookeeper host
   * @param indexName The name of the index
   * @param params The given parameters inside the search decorator
   */
  public MultipleBucketsSearchStream(List<String> bucketNames, String zkHost,
                                     String indexName, SolrParams params) throws IOException {
    super(zkHost, indexName, params);
    bucketSearchStreamWrappers = new LinkedList<>();
    for (String bucketName : bucketNames) {
      bucketSearchStreamWrappers.add(new BucketSearchStreamWrapper(zkHost, indexName, bucketName, params));
    }
  }

  /**
   * creates the streams for each bucket at opening
   */
  protected void constructStreams() throws IOException {
    for (BucketSearchStreamWrapper searchStreamWrapper : bucketSearchStreamWrappers) {
      searchStreamWrapper.setStreamContext(streamContext);
      solrStreams.add(searchStreamWrapper);
    }
  }

  /**
   * utilizes CloudSolrStream read() to ensure sort order between streams
   */
  @Override
  public Tuple read() throws IOException {
    Tuple tuple;
    if (solrStreams.size() != 0) {
      tuple = super.read();
    } else {
      tuple = SolrObjectMocker.mockEOFTuple();
    }
    return tuple;
  }
}
