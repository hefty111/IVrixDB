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

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;

/**
 * This metric counts the number of distinct values in a field. Since distinct count is a non-
 * additive operation (see {@link org.apache.solr.ivrixdb.search.stream.stats.NonAdditiveMetric},)
 * the distinct values are passed on in the tuple if the streaming request is distributed.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: optimization (re-thinking class) -- think about what to do when cardinality of field is high
 */
public class DistinctCountMetric extends NonAdditiveMetric {
  private static final String DISTINCT_VALUES_FIELD_NAME = "__distinct_values";
  private static final String FUNCTION_NAME = "distinctCount";

  private final Set<String> distinctValues = new HashSet<>();
  private final String columnName;

  /**
   * @param columnName The name of the column (field) to count its distinct values.
   */
  public DistinctCountMetric(String columnName) {
    setValuesIdentifier(DISTINCT_VALUES_FIELD_NAME, "(", columnName, ")");
    setIdentifier(FUNCTION_NAME, "(", columnName, ")");
    setFunctionName(FUNCTION_NAME);
    this.columnName = columnName;
  }

  /**
   * Adds the value of the field to the set of distinct values.
   * If conditions apply, it will merge previous results into this one.
   */
  @Override
  public void update(Tuple tuple) {
    List<String> distinctValuesFromTuple = tuple.getStrings(DISTINCT_VALUES_FIELD_NAME);
    String columnValue = tuple.getString(columnName);

    if (distinctValuesFromTuple != null) {
      distinctValues.addAll(distinctValuesFromTuple);
    }

    if (columnValue != null) {
      distinctValues.add(columnValue);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] getNonAdditiveValues() {
    return distinctValues.toArray();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Number getValue() {
    return distinctValues.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getColumns() {
    return new String[]{columnName};
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Metric newInstance() {
    return new DistinctCountMetric(columnName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName);
  }
}
