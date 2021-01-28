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
import org.apache.solr.ivrixdb.utilities.Constants;

/**
 * This object generates summaries of fields by
 * processing one event at a time, dynamically
 * adding a new field summaries, and updating
 * the metrics of each field summary.
 *
 * @author Ivri Faitelson
 */
public class FieldSummarizer {
  private final Map<String, FieldSummary> fieldSummaries;

  /**
   * Creates a FieldSummarizer with default values.
   */
  public FieldSummarizer() {
    this.fieldSummaries = new HashMap<>();
  }

  /**
   * reverse all digestion of events.
   */
  public void reset() {
    fieldSummaries.clear();
  }

  /**
   * Digests an event updating each field summary for the list of fields in the event.
   * Ignores fields from index.
   */
  public void digestEvent(Tuple event) {
    for (String fieldName : (Set<String>)event.fields.keySet()) {
      if (fieldName.equals(Constants.IVrix.RAW_TIME_FIELD) || fieldName.equals(Constants.IVrix.RAW_EVENT_FIELD)) {
        continue;
      }

      FieldSummary fieldSummary = fieldSummaries.computeIfAbsent(fieldName, key -> new FieldSummary(key, event.get(key)));
      fieldSummary.digestEvent(event);
    }
  }

  /**
   * @return The generated summaries of fields.
   */
  public Collection<FieldSummary> getSummaries() {
    return fieldSummaries.values();
  }
}
