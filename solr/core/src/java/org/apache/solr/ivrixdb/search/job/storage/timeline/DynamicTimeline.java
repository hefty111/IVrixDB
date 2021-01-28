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

import java.time.OffsetDateTime;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.ivrixdb.search.job.storage.StoragePushProperties.PushMethod;
import org.apache.solr.ivrixdb.utilities.*;

/**
 * Defines a timeline that digests events, places them
 * into buckets based on a scope, automatically changes
 * scope when necessary, and decides whether or not to
 * store events. Events from a non-stream push are treated
 * differently, because the timeline will need to be re-built
 * each time there is a new non-stream push.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: optimize data structure that holds the events. Is it truly necessary to sort?
 *       perhaps take advantage of the fact that the incoming stream will be sorted for most of the time...
 */
public class DynamicTimeline {
  private static final int maximumStoredEventsWithNonStreamPush = Constants.IVrix.Limitations.Timeline.MAXIMUM_STORED_EVENTS_WITH_NON_STREAM_PUSH;
  private static final int maximumBuckets = Constants.IVrix.Limitations.Timeline.MAXIMUM_TIME_BUCKETS;

  private final Comparator<Tuple> digestOrder;

  private SortedMap<OffsetDateTime, TimeBucket> buckets;
  private TimeBucket allInclusiveBucket;
  private Scope scope;

  /**
   * Creates a timeline that digests events in the given order with default parameters.
   */
  public DynamicTimeline(Comparator<Tuple> digestOrder) {
    this.digestOrder = digestOrder;
    this.buckets = new TreeMap<>(new ReverseTimeComparator());
    reset();
  }

  /**
   * resets all digestion of events, from buckets to scope.
   */
  public void reset() {
    this.allInclusiveBucket = new TimeBucket(digestOrder);
    this.scope = Scope.MILLISECOND_1;
    this.buckets.clear();
  }

  /**
   * digests an event.
   * For digestion, if there is no buckets, create one and add the event there;
   * If the event can't fit into the last bucket, create one and add the event there;
   * If the scope needs to be changed, zoom out one level and merge the buckets.
   *
   * For storage, if the decided bucket has reached its limit, don't store;
   * If the decided bucket has not reached its limit, store the event;
   * If the digestion is through a non-stream push, do not store if
   * non-stream storage limit has been reached.
   */
  public void digestEvent(Tuple event, PushMethod pushMethod) {
    OffsetDateTime eventDateTime = EventTimestampHelper.getDateTime(event);
    OffsetDateTime bucketLowerBound = scope.createLowerBound(eventDateTime);

    TimeBucket bucket = buckets.get(bucketLowerBound);
    boolean bucketCanFitEvent = bucket != null && bucket.canFitEvent(event);

    if (bucket == null || !bucketCanFitEvent) {
      bucket = new TimeBucket(event, scope, digestOrder);
      buckets.put(bucket.getLowerBound(), bucket);

    } else {
      bucket.add(event);
    }
    allInclusiveBucket.add(event);

    storeEventIfPossible(event, bucket, pushMethod);
    changeScopeIfNecessary();
  }

  private void storeEventIfPossible(Tuple event, TimeBucket bucket, PushMethod pushMethod) {
    if (pushMethod == PushMethod.NON_STREAM) {
      if (allInclusiveBucket.getEventCount() > maximumStoredEventsWithNonStreamPush) {
        return;
      }
    }
    allInclusiveBucket.storeIfNotFull(event);
    bucket.storeIfNotFull(event);
  }

  private void changeScopeIfNecessary() {
    if (buckets.size() > maximumBuckets) {
      scope = scope.zoomOut();
      mergeBuckets();
      changeScopeIfNecessary();
    }
  }

  private void mergeBuckets() {
    SortedMap<OffsetDateTime, TimeBucket> mergedBuckets = new TreeMap<>(new ReverseTimeComparator());
    TimeBucket mergedBucket = null;

    for (TimeBucket bucket : buckets.values()) {
      OffsetDateTime lowerBound = (mergedBucket != null) ? mergedBucket.getLowerBound() : null;
      OffsetDateTime earliestInBucket = bucket.getEarliest();

      boolean isNewBucketOutOfMergeRange = lowerBound != null && earliestInBucket.isBefore(lowerBound);
      boolean noMergedBucket = mergedBucket == null;

      if (noMergedBucket || isNewBucketOutOfMergeRange) {
        mergedBucket = new TimeBucket(scope, earliestInBucket, digestOrder);
        mergedBuckets.put(mergedBucket.getLowerBound(), mergedBucket);
      }

      mergedBucket.mergeWith(bucket);
    }

    for (TimeBucket bucket : mergedBuckets.values()) {
      bucket.trimStorage();
    }

    buckets = mergedBuckets;
  }

  /**
   * @return an iterator over the stored events in the "all-inclusive" bucket.
   */
  public Iterator<Tuple> getAllInclusiveIterator() {
    return allInclusiveBucket.getStoredEventsIterator();
  }

  /**
   * @return An iterator over the buckets at the given date time range.
   */
  public Iterator<Tuple> getSelectedBucketsIterator(OffsetDateTime earliest, OffsetDateTime latest) {
    Collection<TimeBucket> selectedBuckets = selectBuckets(earliest, latest);
    return new CombinedBucketsIterator(selectedBuckets, digestOrder);
  }

  /**
   * @return All the buckets except the "all-inclusive" bucket.
   */
  public Collection<TimeBucket> getBuckets() {
    return buckets.values();
  }

  /**
   * @return The "all-inclusive" bucket.
   */
  public TimeBucket getAllInclusiveBucket() {
    return allInclusiveBucket;
  }

  /**
   * @return The scope of the timeline.
   */
  public Scope getScope() {
    return scope;
  }

  private Collection<TimeBucket> selectBuckets(OffsetDateTime earliest, OffsetDateTime latest) {
    List<OffsetDateTime> lowerBoundsOfBuckets = scope.getLowerBoundsBetween(earliest, latest);
    List<TimeBucket> selectedBuckets = new LinkedList<>();
    for (OffsetDateTime lowerBoundOfBucket : lowerBoundsOfBuckets) {
      selectedBuckets.add(buckets.get(lowerBoundOfBucket));
    }
    return selectedBuckets;
  }

  private static class ReverseTimeComparator implements Comparator<OffsetDateTime> {
    @Override
    public int compare(OffsetDateTime a, OffsetDateTime b) {
      return b.compareTo(a);
    }
  }
}
