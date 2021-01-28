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

package org.apache.solr.ivrixdb.search.stream.stats;

import org.apache.solr.client.solrj.io.stream.metrics.Metric;

/**
 * This class represents non-additive metrics.
 * Non-additive metrics are metrics that require
 * data from all tuples. This class had to be made
 * because Solr's distributed rollup cannot support
 * merges between non-additive operations.
 *
 * @author Ivri Faitelson
 */
public abstract class NonAdditiveMetric extends Metric {
  private String valuesIdentifier;

  /**
   * @return The values identifier field name. This field stores
   *         the values associated with this operation.
   */
  public String getValuesIdentifier(){
    return valuesIdentifier;
  }

  /**
   * @param valuesIdentifier The name of the values identifier field name.
   */
  public void setValuesIdentifier(String valuesIdentifier){
    this.valuesIdentifier = valuesIdentifier;
  }

  /**
   * @param identifierParts String parts that when added together, produce the values identifier field name.
   */
  public void setValuesIdentifier(String ... identifierParts){
    StringBuilder sb = new StringBuilder();
    for(String part : identifierParts){
      sb.append(part);
    }
    this.valuesIdentifier = sb.toString();
  }

  /**
   * @return The values associated with this metric operation.
   */
  public abstract Object[] getNonAdditiveValues();
}
