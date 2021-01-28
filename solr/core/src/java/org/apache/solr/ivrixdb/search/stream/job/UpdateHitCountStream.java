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
import org.apache.solr.ivrixdb.utilities.Constants;

/**
 * This decorator informs the search job whenever an event
 * has been found. Attaches to the search pipeline right
 * after the search decorator. Returns tuples unharmed
 * in the pipeline.
 *
 * @author Ivri Faitelson
 */
public class UpdateHitCountStream extends TupleStream implements Expressible {
  private TupleStream innerStream;
  private SearchJob searchJob;

  /**
   * Creates from stream expression
   */
  public UpdateHitCountStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamSegments = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    validate(streamSegments);
    this.innerStream = factory.constructStream(streamSegments.get(0));
  }

  private void validate(List<StreamExpression> streamSegments) throws IOException {
    boolean isStreamSegmentsValidLength = streamSegments.size() == 1;
    if (!isStreamSegmentsValidLength) {
      throw new IOException("Invalid parameters");
    }
  }

  /**
   * Creates from explicit config
   */
  public UpdateHitCountStream(TupleStream innerStream, SearchJob searchJob) {
    this.searchJob = searchJob;
    this.innerStream = innerStream;
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
    innerStream.open();
  }

  /**
   * Reads from the inner stream and returns the tuple unchanged.
   * If the tuple is not EOF, it signals to the
   * search job that an event has been found.
   */
  public Tuple read() throws IOException {
    Tuple tuple = innerStream.read();
    if (!tuple.EOF) {
      searchJob.receiveSignalForEventHit();
    }
    return tuple;
  }

  /**
   * Closes inner stream
   */
  public void close() throws IOException {
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
