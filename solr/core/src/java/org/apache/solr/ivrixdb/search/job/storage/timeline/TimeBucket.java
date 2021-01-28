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

package org.apache.solr.ivrixdb.search.job.storage.timeline;

import java.time.*;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.ivrixdb.search.utilities.ComparableTuple;
import org.apache.solr.ivrixdb.utilities.*;

/**
 * Represents a time-based bucket. Holds the timestamps
 * of the bounds, the earliest and latest events, the
 * number of events in bucket, and the stored events
 * in the bucket.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: optimize data structure that holds the events. Is it truly necessary to sort?
 *       perhaps take advantage of the fact that the incoming stream will be sorted for most of the time...
 */
public class TimeBucket {
  private static final int maximumStoredEvents = Constants.IVrix.Limitations.Timeline.MAXIMUM_STORED_EVENTS_PER_BUCKET;

  private final TreeSet<ComparableTuple> storedEvents;
  private final Comparator<Tuple> digestOrder;
  private final boolean bounded;

  private OffsetDateTime lowerBound;
  private OffsetDateTime upperBound;
  private int eventCount = 0;
  private OffsetDateTime earliest;
  private OffsetDateTime latest;

  public TimeBucket(Comparator<Tuple> digestOrder, boolean bounded) {
    this.storedEvents = new TreeSet<>();
    this.digestOrder = digestOrder;
    this.bounded = bounded;
  }

  /**
   * creates an empty boundless bucket
   */
  public TimeBucket(Comparator<Tuple> digestOrder) {
    this(digestOrder, false);
  }

  /**
   * creates a bounded bucket with one event
   */
  public TimeBucket(Tuple event, Scope scope, Comparator<Tuple> digestOrder) {
    this(digestOrder, true);
    add(event);
    setBounds(scope, earliest);
  }

  /**
   * creates an empty bounded bucket
   */
  public TimeBucket(Scope scope, OffsetDateTime locale, Comparator<Tuple> digestOrder) {
    this(digestOrder, true);
    setBounds(scope, locale);
  }

  /**
   * @return whether an event can fit within the current bounds
   *         NOTE: lower bound inclusive
   */
  public boolean canFitEvent(Tuple event) {
    boolean canFit = true;
    if (bounded) {
      OffsetDateTime eventDateTime = EventTimestampHelper.getDateTime(event);
      canFit = eventDateTime.compareTo(upperBound) < 0 && eventDateTime.compareTo(lowerBound) >= 0;
    }
    return canFit;
  }

  /**
   * updates current properties with an event
   */
  public void add(Tuple event) {
    OffsetDateTime eventDateTime = EventTimestampHelper.getDateTime(event);
    maybeUpdateEarliest(eventDateTime);
    maybeUpdateLatest(eventDateTime);
    eventCount += 1;
  }

  /**
   * merges current properties with a bucket
   */
  public void mergeWith(TimeBucket bucket) {
    maybeUpdateEarliest(bucket.earliest);
    maybeUpdateLatest(bucket.latest);
    eventCount += bucket.eventCount;
    storedEvents.addAll(bucket.storedEvents);
  }

  /**
   * stores the event into the bucket if possible
   * (does not increase the event count.)
   */
  public void storeIfNotFull(Tuple event) {
    storedEvents.add(new ComparableTuple(event, digestOrder));
    if (storedEvents.size() > maximumStoredEvents) {
      storedEvents.pollLast();
    }
  }

  /**
   * trims the stored events to make sure the
   * store is not larger than the set limit.
   */
  public void trimStorage() {
    while (storedEvents.size() > maximumStoredEvents) {
      storedEvents.pollLast();
    }
  }

  private void setBounds(Scope scope, OffsetDateTime locale) {
    lowerBound = scope.createLowerBound(locale);
    upperBound = scope.createUpperBound(lowerBound);
  }

  private void maybeUpdateEarliest(OffsetDateTime maybeNewEarliest) {
    earliest = (earliest == null || maybeNewEarliest.compareTo(earliest) < 0) ? maybeNewEarliest : earliest;
  }

  private void maybeUpdateLatest(OffsetDateTime maybeNewLatest) {
    latest = (latest == null || maybeNewLatest.compareTo(latest) > 0) ? maybeNewLatest : latest;
  }



  /**
   * @return An iterator over the stored events of the bucket
   */
  public Iterator<Tuple> getStoredEventsIterator() {
    return ComparableTuple.createIteratorForComparableTuples(storedEvents);
  }

  /**
   * @return the lower bound of the bucket
   */
  public OffsetDateTime getLowerBound() {
    return lowerBound;
  }

  /**
   * @return the upper bound of the bucket
   */
  public OffsetDateTime getUpperBound() {
    return upperBound;
  }

  /**
   * @return the earliest event in bucket
   */
  public OffsetDateTime getEarliest() {
    return earliest;
  }

  /**
   * @return the latest event in bucket
   */
  public OffsetDateTime getLatest() {
    return latest;
  }

  /**
   * @return the number of events in bucket
   */
  public int getEventCount() {
    return eventCount;
  }

  /**
   * @return is the bucket bounded
   */
  public boolean isBounded() {
    return bounded;
  }
}
