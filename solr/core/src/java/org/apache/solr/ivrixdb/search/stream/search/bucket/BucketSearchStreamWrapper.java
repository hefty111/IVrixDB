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
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.ivrixdb.core.overseer.request.*;
import org.apache.solr.ivrixdb.search.job.SearchJob;
import org.apache.solr.ivrixdb.utilities.*;

/**
 * This stream wraps a standard bucket search stream.
 * It ensures that the bucket is "held" physically before it is being searched.
 * The bucket hold is released at closing.
 *
 * @author Ivri Faitelson
 */
public class BucketSearchStreamWrapper extends SolrStream {
  private final BucketSearchStream bucketSearchStream;
  private final String indexName;
  private final String bucketName;

  private IVrixOverseerOperationParams holdOperationParams;
  private IVrixOverseerOperationParams releaseOperationParams;

  /**
   * @param zkHost The Zookeeper host
   * @param indexName The name of the index
   * @param bucketName The name of the bucket
   * @param params The given parameters inside the search decorator
   */
  public BucketSearchStreamWrapper(String zkHost, String indexName, String bucketName, SolrParams params) throws IOException {
    super(null, null);
    this.bucketSearchStream = new BucketSearchStream(zkHost, bucketName, params);
    this.bucketName = bucketName;
    this.indexName = indexName;
  }

  /**
   * Sets the context of the stream (includes Search Job ID that will be used in hold/release commands)
   */
  @Override
  public void setStreamContext(StreamContext context) {
    SearchJob searchJob = (SearchJob)context.get(Constants.IVrix.SEARCH_JOB_IN_CONTEXT);
    String jobID = searchJob.getJobID();
    this.holdOperationParams = IVrixOverseerOperationParams.HoldBucketForSearch(indexName, bucketName, jobID);
    this.releaseOperationParams = IVrixOverseerOperationParams.ReleaseBucketHold(indexName, bucketName, jobID);
    bucketSearchStream.setStreamContext(context);
  }

  /**
   * Holds bucket before opening bucket stream
   */
  @Override
  public void open() throws IOException {
    boolean doesHaveHoldOnBucket = (boolean)executeOverseerOperation(holdOperationParams);
    if (!doesHaveHoldOnBucket) {
      throw new IOException("Failed to hold bucket: " + bucketName);
    }
    bucketSearchStream.open();
  }

  /**
   * reads from bucket stream
   */
  @Override
  public Tuple read() throws IOException {
    try {
      return bucketSearchStream.read();
    } catch (Exception e) {
      throw new IOException("Failed while reading bucket: " + bucketName, e);
    }
  }

  /**
   * Closes bucket stream before releasing bucket
   */
  @Override
  public void close() throws IOException {
    bucketSearchStream.close();
    executeOverseerOperation(releaseOperationParams);
  }

  private Object executeOverseerOperation(IVrixOverseerOperationParams operation) throws IOException {
    try {
      return IVrixOverseerClient.execute(operation);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }



  /**
   * {@inheritDoc}
   */
  @Override
  public StreamComparator getStreamSort(){
    return bucketSearchStream.getStreamSort();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<TupleStream> children() {
    return bucketSearchStream.children();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return bucketSearchStream.toExplanation(factory);
  }
}
