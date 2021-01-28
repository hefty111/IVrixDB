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
import org.apache.solr.ivrixdb.utilities.Constants;

/**
 * This Metric retrieves the frequency and percentage of the top
 * values in a field.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: refactor -- think about the returned object in getTopValues()
 */
public class TopValuesMetric extends Metric {
  private static final int MAP_LIMIT = Constants.IVrix.Limitations.TopValues.MAXIMUM_DISTINCT_VALUES;
  private static final String FUNCTION_NAME = "topValues";

  private final int returnLimit;
  private final String columnName;

  private final Map<String, Integer> valueToCountMap = new HashMap<>();
  private int totalCount = 0;

  /**
   * @param columnName The name of the column (field) to find the top values of.
   * @param returnLimit The number of top values to retrieve.
   */
  public TopValuesMetric(String columnName, int returnLimit) {
    setIdentifier(FUNCTION_NAME, "(", columnName, ")");
    setFunctionName(FUNCTION_NAME);
    this.columnName = columnName;
    this.returnLimit = returnLimit;
  }

  /**
   * Updates the value-to-frequency mapping given a tuple.
   * Tuple will be ignored if value-to-frequency mapping size
   * has reached its limit, and tuple has a new unique value.
   */
  @Override
  public void update(Tuple tuple) {
    String fieldValue = tuple.getString(columnName);
    if (valueToCountMap.size() < MAP_LIMIT) {
      valueToCountMap.putIfAbsent(fieldValue, 0);
      incrementCount(fieldValue);

    } else {
      if (valueToCountMap.get(fieldValue) != null) {
        incrementCount(fieldValue);
      }
    }
  }

  private void incrementCount(String fieldValue) {
    valueToCountMap.put(fieldValue, valueToCountMap.get(fieldValue) + 1);
    totalCount += 1;
  }

  /**
   * @return The top N (return limit) values in order, with percentage and frequency.
   */
  public List<TopFieldValue> getTopValues() {
    TreeSet<TopFieldValue> sortedFieldValues = new TreeSet<>();
    for (String value : valueToCountMap.keySet()) {
      sortedFieldValues.add(new TopFieldValue(value, valueToCountMap.get(value), totalCount));

      if (sortedFieldValues.size() > returnLimit) {
        sortedFieldValues.pollLast();
      }
    }

    return new LinkedList<>(sortedFieldValues);
  }

  public static class TopFieldValue implements Comparable<TopFieldValue> {
    private final String value;
    private final int count;
    private final int totalCount;

    public TopFieldValue(String value,int count, int totalCount) {
      this.value = value;
      this.count = count;
      this.totalCount = totalCount;
    }

    @Override
    public int compareTo(TopFieldValue o) {
      if (this == o) {
        return 0;
      }
      return this.count >= o.count ? 1 : -1;
    }

    public String getValue() {
      return value;
    }

    public int getCount() {
      return count;
    }

    public double getPercentage() {
      return ((double)count / (double)totalCount) * 100;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Number getValue() {
    return totalCount;
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
