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

package org.apache.solr.ivrixdb.utilities;

import java.time.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.common.*;
import org.apache.solr.common.params.*;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.TimeBounds;

/**
 * This class is a collection of event timestamp functions.
 * It was made in order to create a more global perspective of
 * what happens to a timestamp at index-time and search-time
 *
 * @author Ivri Faitelson
 */
public class EventTimestampHelper {

  /**
   * @param event an event from the Streaming API
   * @return The timestamp of the event as a date time object
   */
  public static OffsetDateTime getDateTime(Tuple event) {
    long dateTimeLong = event.getLong(Constants.IVrix.RAW_TIME_FIELD);
    return OffsetDateTime.from(Instant.ofEpochSecond(dateTimeLong).atZone(ZoneOffset.UTC));
  }

  /**
   * @param event An event to be indexed
   * @return The timestamp of the event as a date time object
   */
  public static OffsetDateTime getDateTime(SolrInputDocument event) {
    String dateTimeString = (String)event.getFieldValue(Constants.IVrix.RAW_TIME_FIELD);
    return OffsetDateTime.parse(dateTimeString);
  }

  /**
   * @param event An event to be indexed
   * @return the modified event with a unix timestamp
   */
  public static SolrInputDocument createModifiedEventWithUnixTimestamp(SolrInputDocument event) {
    SolrInputDocument modifiedEvent = event.deepCopy();

    SolrInputField newTimeField = new SolrInputField(Constants.IVrix.RAW_TIME_FIELD);
    newTimeField.setValue(getDateTime(event).toEpochSecond());

    modifiedEvent.removeField(Constants.IVrix.RAW_TIME_FIELD);
    modifiedEvent.put(Constants.IVrix.RAW_TIME_FIELD, newTimeField);
    return modifiedEvent;
  }

  /**
   * @param params The search params
   * @param searchRange The time range of the search
   * @return The modified search params with the time range included
   */
  public static SolrParams createModifiedSearchParamsWithSearchRange(SolrParams params, TimeBounds searchRange) {
    ModifiableSolrParams modifiedParams = new ModifiableSolrParams(params);

    String qParam = modifiedParams.get("q");
    qParam = searchRange.toSolrQuerySyntax() + (qParam.length() == 0 ? "" : " AND ") +  qParam;

    modifiedParams.remove("q");
    modifiedParams.add("q", qParam);
    return modifiedParams;
  }
}
