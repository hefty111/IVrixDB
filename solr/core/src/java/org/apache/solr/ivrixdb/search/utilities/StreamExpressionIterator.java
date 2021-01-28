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

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;

/**
 * This iterator enables iteration over a Solr Streaming Expression.
 * holds enough information for visitors to be able to modify,
 * insert, and remove expressions in a Streaming Expression
 * on the fly.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: Clean up nextIndex and currentIndex values (what are they? I confused myself...)
 */
public class StreamExpressionIterator implements Iterator<StreamExpression> {
  public StreamExpression previousLayer;
  public int currentLayerIndex;
  public StreamExpression currentLayer;
  public int nextLayerIndex;
  public boolean firstIteration;

  /**
   * @param streamExpression The streaming expression to iterate over
   */
  public StreamExpressionIterator(StreamExpression streamExpression) {
    this.nextLayerIndex = -1;
    this.currentLayerIndex = -1;
    this.previousLayer = null;
    this.firstIteration = true;
    this.currentLayer = streamExpression;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasNext() {
    List<StreamExpressionParameter> currentLayerParameters = currentLayer.getParameters();
    boolean hasStreamExpressionParameter = false;
    for (int i = 0; i < currentLayerParameters.size(); i++) {
      StreamExpressionParameter parameter = currentLayerParameters.get(i);
      if (parameter instanceof StreamExpression) {
        hasStreamExpressionParameter = true;
        nextLayerIndex = i;
        break;
      }
    }

    if (firstIteration) {
      hasStreamExpressionParameter = true;
    }

    return hasStreamExpressionParameter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamExpression next() {
    if (!firstIteration) {
      previousLayer = currentLayer;
      currentLayerIndex = nextLayerIndex;
      currentLayer = (StreamExpression)currentLayer.getParameters().get(nextLayerIndex);

    } else {
      firstIteration = false;
    }

    return currentLayer;
  }
}
