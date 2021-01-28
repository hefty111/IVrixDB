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

package org.apache.solr.ivrixdb.search.utilities;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.ivrixdb.search.stream.nonstream.*;

/**
 * This utility enables creations of streaming pipelines for IVrixDB,
 * such as the search and preview pipelines. The search pipeline
 * executes the query as the user demanded, sending information
 * to the Search Job wherever necessary in the pipeline. The
 * preview pipeline is a reusable pipeline that returns a
 * snapshot of the search pipeline whenever called upon,
 * sending information to the Search Job wherever necessary
 * in the pipeline. The preview pipeline returns snapshots of
 * the search pipeline by inspecting components in the search pipeline
 * that are non-streaming. Non-streaming components are components
 * that require the entire data-set to be processed in order to
 * pass the data onto the next component in the pipeline.
 *
 * @author Ivri Faitelson
 */
public class StreamBuilder {
  /**
   * Constructs the search pipeline by copying the main expression
   * and attaching search job informers wherever necessary.
   */
  public static TupleStream buildSearchStream(StreamExpression expression, StreamFactory streamFactory) throws IOException {
    StreamExpression mainExpression = StreamExpressionModifier.copyExpression(expression);
    mainExpression = StreamExpressionModifier.attachHitCounter(mainExpression);
    mainExpression = StreamExpressionModifier.attachStoragePushers(mainExpression);
    return streamFactory.constructStream(mainExpression);
  }


  /**
   * Constructs the preview pipeline by copying the main expression,
   * changing all non-streaming expressions to non-stream preview expressions,
   * attaching search job informers wherever necessary, and cutting off the rest
   * of the pipeline at the last non-streaming decorator.
   *
   * Will return null if no preview pipeline can be constructed.
   */
  public static TupleStream buildPreviewStream(StreamExpression expression, StreamFactory streamFactory) throws IOException {
    StreamExpression previewExpression = StreamExpressionModifier.copyExpression(expression);
    previewExpression = StreamExpressionModifier.attachStoragePushers(previewExpression);
    previewExpression = StreamExpressionModifier.changeAllNonStreamingToPreviewFunctions(previewExpression);
    previewExpression = StreamExpressionModifier.trimPreviewExpression(previewExpression);

    TupleStream previewStream = null;
    if (previewExpression != null) {
      previewStream = streamFactory.constructStream(previewExpression);
    }

    return previewStream;
  }

  /**
   * Bridges a preview and search pipeline so that preview
   * can return a snapshot of the objects within search.
   */
  public static void bridgePreviewWithMain(TupleStream mainStream, TupleStream previewStream) {
    List<NonStreamProcessorStream> nonStreamsFromMain = findAllNonStreamProcessorStreams(mainStream);
    List<NonStreamProcessorStream> nonStreamsFromPreview = findAllNonStreamProcessorStreams(previewStream);

    if (nonStreamsFromMain.size() == nonStreamsFromPreview.size()) {
      for (int i = 0; i < nonStreamsFromPreview.size(); i++) {
        NonStreamPreviewStream nonStreamPreview = (NonStreamPreviewStream)nonStreamsFromPreview.get(i);
        NonStreamProcessorStream nonStreamMain = nonStreamsFromMain.get(i);

        nonStreamPreview.connectBridge(nonStreamMain);
      }
    }
  }

  private static List<NonStreamProcessorStream> findAllNonStreamProcessorStreams(TupleStream stream) {
    List<NonStreamProcessorStream> processorStreams = new ArrayList<>();
    if (stream instanceof NonStreamProcessorStream) {
      processorStreams.add((NonStreamProcessorStream)stream);
    }

    List<TupleStream> children = stream.children();
    if (children != null && children.size() > 0) {
      List<NonStreamProcessorStream> childrenProcessorStreams = findAllNonStreamProcessorStreams(children.get(0));
      processorStreams.addAll(childrenProcessorStreams);
    }

    return processorStreams;
  }
}
