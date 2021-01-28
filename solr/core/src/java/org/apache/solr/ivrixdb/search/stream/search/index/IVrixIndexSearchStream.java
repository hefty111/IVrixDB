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

package org.apache.solr.ivrixdb.search.stream.search.index;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.TimeBounds;
import org.apache.solr.ivrixdb.core.overseer.request.*;
import org.apache.solr.ivrixdb.core.overseer.response.OrganizedBucketsForSearch;
import org.apache.solr.ivrixdb.utilities.EventTimestampHelper;
import org.apache.solr.ivrixdb.search.stream.search.bucket.*;
import org.apache.solr.ivrixdb.utilities.Constants;
import org.apache.solr.ivrixdb.utilities.SolrObjectMocker;

/**
 * This stream searches the buckets of an IVrix index given a search range (or not).
 *
 * The stream will request the Overseer to provide a list of buckets to search.
 * The HOT buckets are searched first, then the WARM buckets, then the COLD buckets.
 * Within each bucket age category, the buckets are searched in reverse time order.
 * Before each bucket is searched, it must be "held", meaning that it has to physically exist
 * and it cannot be lost before the search is over. Afterwards, that hold must be released.
 * This is also requested through the Overseer.
 *
 * Multiple buckets can be searched at once according to a parallelization factor.
 * This, and other factors such as the fact that the time-spans of buckets overlap,
 * means that the search will not be in absolute reverse time order.
 * This is a key aspect of the IVrix search feature, and all other components must be able
 * to handle the nature of this stream.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: fix potential bugs with non-time sort search (due to stream expression push-down to solr core)
 * TODO: think about if ZKHost parameter should even exist...
 */
public class IVrixIndexSearchStream extends TupleStream implements Expressible {
  private final String indexName;
  private final int parallelizationFactor;
  private final String zkHost;
  private final SolrParams params;
  private StreamContext context;

  private SolrParams modifiedParamsWithSearchRange;
  private Iterator<String> bucketsIterator;
  private TupleStream nextBatchedStream;


  /**
   * @param indexName The name of the index
   * @param parallelizationFactor The parallelization factor that determines the number of buckets opened at once
   * @param zkHost The Zookeeper host
   * @param params The given parameters inside the search decorator
   */
  public IVrixIndexSearchStream(String indexName, int parallelizationFactor, String zkHost, SolrParams params) throws IOException {
    this.indexName = indexName;
    this.zkHost = zkHost;
    this.params = params;
    this.parallelizationFactor = parallelizationFactor;
  }

  /**
   * Uses default parallelization factor
   * @param indexName The name of the index
   * @param zkHost The Zookeeper host
   * @param params The given parameters inside the search decorator
   */
  public IVrixIndexSearchStream(String indexName, String zkHost, SolrParams params) throws IOException {
    this.indexName = indexName;
    this.zkHost = zkHost;
    this.params = params;
    this.parallelizationFactor = Constants.IVrix.DEFAULT_PARALLELIZATION_FACTOR;
  }

  /**
   * Sets the context of the stream (includes the given time range)
   */
  @Override
  public void setStreamContext(StreamContext context) {
    this.context = context;
  }


  /**
   * Extracts search range out of the StreamContext, and uses
   * it to request a list of buckets to search from the Overseer.
   * Also modifies the Q search params to include the search range.
   */
  @Override
  public void open() throws IOException {
    TimeBounds searchRange = (TimeBounds)context.get(Constants.IVrix.SEARCH_RANGE_IN_CONTEXT);
    this.modifiedParamsWithSearchRange = EventTimestampHelper.createModifiedSearchParamsWithSearchRange(params, searchRange);

    IVrixOverseerOperationParams getOperationParams = IVrixOverseerOperationParams.GetBucketsForSearchJob(
        indexName, searchRange
    );

    Object operationResponse;
    try {
      operationResponse = IVrixOverseerClient.execute(getOperationParams);
    } catch (Exception e) {
      throw new IOException(e);
    }
    OrganizedBucketsForSearch bucketsForSearch = OrganizedBucketsForSearch.fromOverseerResponse(operationResponse);
    bucketsIterator = bucketsForSearch.iterator();
  }

  /**
   * Uses the buckets list to batch buckets given the parallelization factor
   * and search them in order. Opens batch, and reads from them until EOF.
   * Then does the same for the next batch. Each batch holds and releases the buckets.
   */
  @Override
  public Tuple read() throws IOException {
    if (IVrixLocalNode.isShuttingDown()) {
      return SolrObjectMocker.mockEOFTuple();
    }

    Tuple tuple = null;

    if (nextBatchedStream == null) {
      boolean nextBatchExists = prepareNextBatchedStream();
      if (nextBatchExists) {
        nextBatchedStream.open();
      } else {
        tuple = SolrObjectMocker.mockEOFTuple();
      }
    }

    if (tuple == null) {
      tuple = nextBatchedStream.read();
      if (tuple.EOF) {
        nextBatchedStream.close();
        nextBatchedStream = null;
        tuple = this.read();
      }
    }

    return tuple;
  }

  /**
   * Closes the current batch if it hasn't been closed already (possibly due to failure).
   */
  @Override
  public void close() throws IOException {
    if (nextBatchedStream != null) {
      nextBatchedStream.close();
    }
  }

  private boolean prepareNextBatchedStream() throws IOException {
    List<String> nextBucketsBatch = new LinkedList<>();
    while (bucketsIterator.hasNext() && nextBucketsBatch.size() < parallelizationFactor) {
      String nextBucket = bucketsIterator.next();
      nextBucketsBatch.add(nextBucket);
    }

    boolean nextBatchExists = nextBucketsBatch.size() > 0;
    if (nextBatchExists) {
      nextBatchedStream = createBatchedStream(nextBucketsBatch, zkHost, indexName, modifiedParamsWithSearchRange);
      nextBatchedStream.setStreamContext(context);
    }
    return nextBatchExists;
  }

  private TupleStream createBatchedStream(List<String> bucketNames, String zkHost,
                                          String indexName, SolrParams params) throws IOException {
    if (bucketNames.size() == 1) {
      return new BucketSearchStreamWrapper(zkHost, indexName, bucketNames.get(0), params);
    } else {
      return new MultipleBucketsSearchStream(bucketNames, zkHost, indexName, params);
    }
  }



  /**
   * {@inheritDoc}
   */
  @Override
  public List<TupleStream> children() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return null;
  }
}
