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

package org.apache.solr.ivrixdb.search.stream.nonstream;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.ivrixdb.utilities.SolrObjectMocker;

/**
 * This stream holds the commonalities between all of IVrixDB's non-streaming decorators.
 * It uses a non-stream processor to process all tuples from downstream, and returns
 * the results one by one.
 *
 * @author Ivri Faitelson
 */
public abstract class NonStreamProcessorStream extends TupleStream implements Expressible {
  protected NonStreamProcessor processor;
  protected TupleStream innerStream;
  private Iterator<Tuple> generatedTupleIterator;

  /**
   * Opens the inner stream, processes the stream,
   * changes processor state when finished, and
   * closes the inner stream.
   */
  @Override
  public void open() throws IOException {
    innerStream.open();
    processStream();
    processor.finished();
    innerStream.close();

    generatedTupleIterator = processor.generatedTuplesIterator();
  }

  private void processStream() throws IOException {
    Tuple next = innerStream.read();
    while (!next.EOF) {
      processor.process(next);
      next = innerStream.read();
    }
  }

  /**
   * iterates over the results of the processor.
   */
  @Override
  public Tuple read() throws IOException {
    Tuple tuple;

    if (generatedTupleIterator.hasNext()) {
      tuple = generatedTupleIterator.next();
      generatedTupleIterator.remove();

    } else {
      tuple = SolrObjectMocker.mockEOFTuple();
    }

    return tuple;
  }

  /**
   * Does nothing for now. Not necessary to be called
   * for the functionality of the object.
   */
  @Override
  public void close() throws IOException {}


  /**
   * returns the processor.
   */
  public NonStreamProcessor getProcessor() {
    return processor;
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
  @Override
  public void setStreamContext(StreamContext context) {
    this.innerStream.setStreamContext(context);
  }
}
