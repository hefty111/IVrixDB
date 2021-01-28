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

package org.apache.solr.ivrixdb.search.stream.job;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.ivrixdb.search.job.SearchJob;
import org.apache.solr.ivrixdb.search.job.storage.StoragePushProperties;
import org.apache.solr.ivrixdb.search.job.storage.StoragePushProperties.PushMethod;
import org.apache.solr.ivrixdb.search.job.storage.SearchJobStorage.StorageType;

import org.apache.solr.ivrixdb.utilities.Constants;


/**
 * This decorator implements a splitter in a pipeline.
 * It pushes the tuples to the search job.
 * Returns tuples unharmed in the pipeline.
 *
 * @author Ivri Faitelson
 */
public class PushTuplesToJobStream extends TupleStream implements Expressible {
  private StoragePushProperties pushProperties;
  private TupleStream innerStream;
  private PushMethod pushMethod;
  private StorageType storageType;
  private SearchJob searchJob;

  private boolean hasSignalledInit = false;

  /**
   * Creates from stream expression
   */
  public PushTuplesToJobStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamSegments = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter storageTypeSegment = factory.getNamedOperand(expression, StorageType.class.getName());
    StreamExpressionNamedParameter pushMethodSegment = factory.getNamedOperand(expression, PushMethod.class.getName());
    validate(streamSegments, storageTypeSegment, pushMethodSegment);

    StreamExpression innerStreamExpression = streamSegments.get(0);
    String storageTypeName = ((StreamExpressionValue)storageTypeSegment.getParameter()).getValue();
    String pushMethodName = ((StreamExpressionValue)pushMethodSegment.getParameter()).getValue();

    this.innerStream = factory.constructStream(innerStreamExpression);
    this.storageType = StorageType.valueOf(storageTypeName);
    this.pushMethod = PushMethod.valueOf(pushMethodName);
  }

  private void validate(List<StreamExpression> streamSegments, StreamExpressionNamedParameter storageTypeSegment,
                        StreamExpressionNamedParameter pushMethodSegment) throws IOException {
    boolean isStreamSegmentsValidLength = streamSegments.size() == 1;
    boolean isStorageTypeSegmentValidType = storageTypeSegment.getParameter() instanceof StreamExpressionValue;
    boolean isPushMethodSegmentValidType = pushMethodSegment.getParameter() instanceof StreamExpressionValue;

    if (!isStreamSegmentsValidLength || !isStorageTypeSegmentValidType || !isPushMethodSegmentValidType) {
      throw new IOException("Invalid parameters");
    }
  }

  /**
   * Creates from explicit config
   */
  public PushTuplesToJobStream(TupleStream innerStream, SearchJob searchJob, StorageType storageType, PushMethod pushMethod) {
    this.searchJob = searchJob;
    this.innerStream = innerStream;
    this.storageType = storageType;
    this.pushMethod = pushMethod;
  }

  /**
   * Grabs the Search Job from the stream context.
   */
  @Override
  public void setStreamContext(StreamContext context) {
    this.searchJob = (SearchJob)context.get(Constants.IVrix.SEARCH_JOB_IN_CONTEXT);
    this.innerStream.setStreamContext(context);
  }

  /**
   * Opens inner stream
   */
  @Override
  public void open() throws IOException {
    if (pushProperties == null) {
      pushProperties = new StoragePushProperties(innerStream.getStreamSort(), storageType, pushMethod);
    }
    innerStream.open();
  }

  /**
   * Reads from the inner stream and returns the tuple unchanged.
   * Pushes the tuple to a configured location in the search job storage.
   */
  public Tuple read() throws IOException {
    Tuple tuple = innerStream.read();

    if (!hasSignalledInit) {
      searchJob.receiveSignalForPushInit(pushProperties);
      hasSignalledInit = true;
    }

    if (!tuple.EOF) {
      searchJob.storeTuple(tuple, pushProperties);
    } else {
      searchJob.receiveSignalForPushFinish(pushProperties);
    }

    return tuple;
  }

  /**
   * Closes inner stream. Resets the decorator for re-use.
   */
  public void close() throws IOException {
    hasSignalledInit = false;
    innerStream.close();
  }



  /**
   * {@inheritDoc}
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return innerStream.toExplanation(factory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return ((Expressible)innerStream).toExpression(factory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<TupleStream> children() {
    return Collections.singletonList(innerStream);
  }


  /**
   * {@inheritDoc}
   */
  public StreamComparator getStreamSort(){
    return innerStream.getStreamSort();
  }
}
