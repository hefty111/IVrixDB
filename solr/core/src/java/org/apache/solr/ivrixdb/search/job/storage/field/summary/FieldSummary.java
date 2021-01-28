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

package org.apache.solr.ivrixdb.search.job.storage.field.summary;

import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.ivrixdb.search.stream.stats.TopValuesMetric;

/**
 * This object encapsulates a field summary.
 * It can process events one at a time, and
 * generate a list of metrics which comprise
 * of the summary.
 *
 * @author Ivri Faitelson
 */
public class FieldSummary {
  private final String fieldName;
  private final List<Metric> summaryMetrics;
  private final TopValuesMetric summaryTopValues;

  /**
   * @param name The name of the field to generate a summary.
   * @param sampleValue Used to see if the field's value are numbers or strings.
   */
  public FieldSummary(String name, Object sampleValue) {
    this.fieldName = name;
    this.summaryMetrics = new LinkedList<>();

    boolean isNumber = !(sampleValue instanceof String);
    if (isNumber) {
      this.summaryMetrics.add(new MinMetric(fieldName));
      this.summaryMetrics.add(new MaxMetric(fieldName));
      this.summaryMetrics.add(new MeanMetric(fieldName));
    }

    summaryTopValues = new TopValuesMetric(fieldName, 10);
  }

  /**
   * Digests an event and computes summary data for the configured field.
   */
  public void digestEvent(Tuple event) {
    for (Metric summaryMetric : summaryMetrics) {
      summaryMetric.update(event);
    }
    summaryTopValues.update(event);
  }

  /**
   * @return The top values for the configured field.
   */
  public TopValuesMetric getSummaryTopValues() {
    return summaryTopValues;
  }

  /**
   * @return The metrics computed for this field summary.
   */
  public List<Metric> getSummaryMetrics() {
    return summaryMetrics;
  }

  /**
   * @return The name of the field in this field summary.
   */
  public String getFieldName() {
    return fieldName;
  }
}
