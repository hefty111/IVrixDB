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

import java.util.*;

import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.ivrixdb.search.job.storage.StoragePushProperties.PushMethod;
import org.apache.solr.ivrixdb.search.job.storage.SearchJobStorage.StorageType;

/**
 * This utility provides modifications, insertions, and
 * deletions of segments in a Solr Streaming Expression for
 * the creation of IVrixDB streaming pipelines.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: Refactor code to make it look nice and have no side-effects
 * TODO: Refactor code so that many operations can be done in the same iteration (perhaps a list of Operation objects?).
 * TODO: SELECT decorator without _time and _raw fields MUST BE "transforming" decorator.
 */
public class StreamExpressionModifier {
  /**
   * copies a streaming expression, so that any
   * operations on one will not occur in the other.
   */
  public static StreamExpression copyExpression(StreamExpression streamExpression) {
    StreamExpression newExpression = copyLayer(streamExpression);
    StreamExpressionIterator expressionIterator = new StreamExpressionIterator(newExpression);

    while (expressionIterator.hasNext()) {
      expressionIterator.next();
      StreamExpression copiedLayer = copyLayer(expressionIterator.currentLayer);
      replaceCurrent(expressionIterator.previousLayer, expressionIterator.currentLayerIndex, copiedLayer);
    }
    return newExpression;
  }

  /**
   * changes all the functions that are non-streaming
   * to be functions that preview non-streams.
   * (This modification is exclusively for preview pipeline.)
   */
  public static StreamExpression changeAllNonStreamingToPreviewFunctions(StreamExpression streamExpression) {
    StreamExpressionIterator expressionIterator = new StreamExpressionIterator(streamExpression);
    while (expressionIterator.hasNext()) {
      expressionIterator.next();

      String functionName = expressionIterator.currentLayer.getFunctionName();
      boolean isFunctionIVrix = StreamDefinitionsHolder.streamingFunctions.containsKey(functionName);
      boolean isFunctionNonStreaming = isFunctionIVrix && isNonStreamingFunction(functionName);

      if (isFunctionNonStreaming) {
        expressionIterator.currentLayer.setFunctionName(StreamDefinitionsHolder.NON_STREAM_PREVIEW_FUNCTION_NAME);
      }
    }
    return streamExpression;
  }

  /**
   * Trims the streaming expression of wasteful expressions.
   * If all is wasteful, this function will return null.
   * (This modification is exclusively for preview pipeline.)
   */
  public static StreamExpression trimPreviewExpression(StreamExpression streamExpression) {
    StreamExpressionIterator expressionIterator = new StreamExpressionIterator(streamExpression);
    boolean hasNonStream = streamExpression.getFunctionName().equals(StreamDefinitionsHolder.NON_STREAM_PREVIEW_FUNCTION_NAME);

    StreamExpression lastNonStreamPreview = null;
    int indexOfNextOfLastNonStream = -1;

    while (expressionIterator.hasNext()) {
      expressionIterator.next();
      if (expressionIterator.previousLayer == null) {
        continue;
      }

      String previousFunctionName = expressionIterator.previousLayer.getFunctionName();
      boolean isFunctionNonStreamPreview = previousFunctionName.equals(StreamDefinitionsHolder.NON_STREAM_PREVIEW_FUNCTION_NAME);

      if (isFunctionNonStreamPreview) {
        lastNonStreamPreview = expressionIterator.previousLayer;
        indexOfNextOfLastNonStream = expressionIterator.currentLayerIndex;
        hasNonStream = true;
      }
    }

    if (lastNonStreamPreview != null) {
      replaceCurrent(lastNonStreamPreview, indexOfNextOfLastNonStream, null);
    }
    if (!hasNonStream) {
      streamExpression = null;
    }

    return streamExpression;
  }

  /**
   * inserts expressions that push tuples to the Search Job where necessary.
   */
  public static StreamExpression attachStoragePushers(StreamExpression streamExpression) {
    StreamExpression expressionWithAttachedPushers;
    expressionWithAttachedPushers = attachEventStoragePusher(streamExpression);

    if (!expressionWithAttachedPushers.getFunctionName().equals(StreamDefinitionsHolder.PUSH_TO_JOB_FUNCTION_NAME)) {
      expressionWithAttachedPushers = attachResultsStoragePusher(expressionWithAttachedPushers);
    }

    return expressionWithAttachedPushers;
  }

  /**
   * Inserts a Hit Counter expression after a search expression.
   * (This modification is exclusively for search pipeline.)
   */
  public static StreamExpression attachHitCounter(StreamExpression streamExpression) {
    StreamExpressionIterator expressionIterator = new StreamExpressionIterator(streamExpression);
    StreamExpression newExpression = streamExpression;

    while (expressionIterator.hasNext()) {
      expressionIterator.next();
      if (expressionIterator.previousLayer == null) {
        continue;
      }

      String currentFunctionName = expressionIterator.currentLayer.getFunctionName();
      boolean isFunctionIVrix = StreamDefinitionsHolder.streamingFunctions.containsKey(currentFunctionName);
      boolean isFunctionSearch = isFunctionIVrix && currentFunctionName.equals(StreamDefinitionsHolder.SEARCH_FUNCTION_NAME);

      if (isFunctionSearch) {
        StreamExpression hitCounterExpression = new StreamExpression(StreamDefinitionsHolder.UPDATE_HIT_COUNT_FUNCTION_NAME);
        hitCounterExpression.addParameter(expressionIterator.currentLayer);

        if (expressionIterator.previousLayer != null) {
          replaceCurrent(expressionIterator.previousLayer, expressionIterator.currentLayerIndex, hitCounterExpression);
        } else {
          newExpression = hitCounterExpression;
        }
        break;
      }
    }

    return newExpression;
  }





  private static StreamExpression attachEventStoragePusher(StreamExpression streamExpression) {
    StreamExpressionIterator expressionIterator = new StreamExpressionIterator(streamExpression);
    StreamExpression newExpression = streamExpression;
    boolean attached = false;

    while (expressionIterator.hasNext()) {
      expressionIterator.next();
      if (expressionIterator.previousLayer == null) {
        continue;
      }

      String previousFunctionName = expressionIterator.previousLayer.getFunctionName();
      boolean isFunctionIVrix = StreamDefinitionsHolder.streamingFunctions.containsKey(previousFunctionName);
      boolean isFunctionTransforming = isFunctionIVrix && isTransformingFunction(previousFunctionName);

      if (isFunctionTransforming) {
        StreamExpression jobPushExpression = attachStoragePusher(expressionIterator.currentLayer, StorageType.EVENTS);
        attached = replaceCurrent(expressionIterator.previousLayer, expressionIterator.currentLayerIndex, jobPushExpression);
        break;
      }
    }

    if (!attached) {
      newExpression = attachStoragePusher(streamExpression, StorageType.EVENTS);
    }

    return newExpression;
  }

  private static StreamExpression attachResultsStoragePusher(StreamExpression streamExpression) {
    return attachStoragePusher(streamExpression, StorageType.RESULTS);
  }

  private static StreamExpression attachStoragePusher(StreamExpression streamExpression, StorageType storageType) {
    StreamExpression attachedExpression = new StreamExpression(StreamDefinitionsHolder.PUSH_TO_JOB_FUNCTION_NAME);
    attachedExpression.addParameter(streamExpression);
    attachedExpression.addParameter(new StreamExpressionNamedParameter(StorageType.class.getName(), storageType.name()));

    PushMethod pushMethod;
    if (doesDownStreamHaveNonStream(attachedExpression)) {
      pushMethod = PushMethod.NON_STREAM;
    } else {
      pushMethod = PushMethod.STREAM;
    }
    attachedExpression.addParameter(new StreamExpressionNamedParameter(PushMethod.class.getName(), pushMethod.name()));

    return attachedExpression;
  }

  private static boolean doesDownStreamHaveNonStream(StreamExpression storagePusherExpression) {
    StreamExpressionIterator expressionIterator = new StreamExpressionIterator(storagePusherExpression);
    boolean foundNonStream = false;
    while (expressionIterator.hasNext()) {
      expressionIterator.next();

      String functionName = expressionIterator.currentLayer.getFunctionName();
      boolean isFunctionIvrix = StreamDefinitionsHolder.streamingFunctions.containsKey(functionName);
      boolean isFunctionNonStreaming = isFunctionIvrix && isNonStreamingFunction(functionName);

      if (isFunctionNonStreaming) {
        foundNonStream = true;
        break;
      }
    }
    return foundNonStream;
  }




  private static boolean isNonStreamingFunction(String functionName) {
    return StreamDefinitionsHolder.streamingFunctions.get(functionName).isNonStreaming();
  }

  private static boolean isTransformingFunction(String functionName) {
    return StreamDefinitionsHolder.streamingFunctions.get(functionName).isTransforming();
  }


  private static boolean replaceCurrent(StreamExpression expression, int currentIndex, StreamExpression toReplaceWith) {
    boolean replaced = false;
    if (expression != null) {
      List<StreamExpressionParameter> previousLayerParameters = expression.getParameters();
      previousLayerParameters.set(currentIndex, toReplaceWith);
      expression.setParameters(previousLayerParameters);
      replaced = true;
    }
    return replaced;
  }

  private static StreamExpression copyLayer(StreamExpression layer) {
    StreamExpression newLayer = new StreamExpression(layer.getFunctionName());
    newLayer.setParameters(new ArrayList<>(layer.getParameters()));
    return newLayer;
  }
}
