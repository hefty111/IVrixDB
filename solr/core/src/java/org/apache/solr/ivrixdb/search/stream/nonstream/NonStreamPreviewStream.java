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

import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.ivrixdb.utilities.SolrObjectMocker;

/**
 * This stream is a component of the preview pipeline.
 * It attaches to a processor in the search pipeline to
 * provide the capability of snapshotting a non-streaming
 * decorator. Can either snapshot from source, or process
 * tuples from downstream.
 *
 * @author Ivri Faitelson
 */
public class NonStreamPreviewStream extends NonStreamProcessorStream {
  private Iterator<Tuple> sourceTuplesIterator;
  private StreamComparator sourceStreamSortOrder;
  private NonStreamProcessor sourceProcessor;
  private boolean shouldIterateOverSource;

  /**
   * creates from stream expression.
   */
  public NonStreamPreviewStream(StreamExpression expression, StreamFactory factory) throws IOException {
    try {
      List<StreamExpression> streamSegments = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
      validate(streamSegments);
      this.innerStream = factory.constructStream(streamSegments.get(0));

    } catch (NullPointerException e) {
      this.innerStream = null;
    }
  }

  private void validate(List<StreamExpression> streamSegments) throws IOException {
    boolean isStreamSegmentsValidLength = streamSegments.size() == 1;
    if (!isStreamSegmentsValidLength) {
      throw new IOException("Invalid parameters");
    }
  }

  /**
   * Decides whether to iterate over the source processor or
   * copy the processor and processes the inner stream. Depends
   * on whether the previous source processor in the inner stream
   * has finished (or even exists).
   */
  @Override
  public void open() throws IOException {
    shouldIterateOverSource = isPreviousSourceProcessorFinished(this.children());
    if (shouldIterateOverSource) {
      sourceProcessor.lockProcessing();
      sourceTuplesIterator = sourceProcessor.generatedTuplesIterator();

    } else {
      processor = sourceProcessor.createNewWithSameConfig();
      super.open();
    }
  }

  private boolean isPreviousSourceProcessorFinished(List<TupleStream> children) {
    boolean isPreviousSourceProcessorFinished = true;
    if (children.size() > 0) {
      TupleStream child = children.get(0);

      if (child instanceof NonStreamPreviewStream) {
        isPreviousSourceProcessorFinished = ((NonStreamPreviewStream)child).isSourceProcessorFinished();
      } else {
        isPreviousSourceProcessorFinished = isPreviousSourceProcessorFinished(child.children());
      }
    }
    return isPreviousSourceProcessorFinished;
  }

  /**
   * Either iterates over the source processor, or over the generated processor.
   */
  @Override
  public Tuple read() throws IOException {
    Tuple nextTuple;

    if (shouldIterateOverSource) {
      if (sourceTuplesIterator.hasNext()) {
        nextTuple = sourceTuplesIterator.next();
      } else {
        nextTuple = SolrObjectMocker.mockEOFTuple();
        sourceProcessor.unlockProcessing();
      }
    } else {
      nextTuple = super.read();
    }

    return nextTuple;
  }

  /**
   * Connects bridge between this previewStream with a processorStream from search pipeline.
   */
  public void connectBridge(NonStreamProcessorStream nonStreamProcessorStream) {
    this.sourceProcessor = nonStreamProcessorStream.getProcessor();
    this.sourceStreamSortOrder = nonStreamProcessorStream.getStreamSort();
  }

  /**
   * Cuts off the rest of the downstream pipeline.
   */
  public void removeInnerStream() {
    this.innerStream = null;
  }

  /**
   * returns the state of the source processor.
   */
  public boolean isSourceProcessorFinished() {
    return sourceProcessor.isFinished();
  }



  /**
   * {@inheritDoc}
   * returns list with innerstream if exists, or empty list if not.
   */
  @Override
  public List<TupleStream> children() {
    List<TupleStream> children;

    if (innerStream != null) {
      children = Collections.singletonList(innerStream);
    } else {
      children = Collections.emptyList();
    }

    return children;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setStreamContext(StreamContext context) {
    if (innerStream != null) {
      this.innerStream.setStreamContext(context);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamComparator getStreamSort() {
    if (innerStream != null) {
      return innerStream.getStreamSort();
    } else {
      return sourceStreamSortOrder;
    }
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
}
