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

package org.apache.solr.ivrixdb.search.job.storage;

import java.time.OffsetDateTime;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;
import org.apache.solr.ivrixdb.search.job.storage.field.summary.*;
import org.apache.solr.ivrixdb.search.job.storage.timeline.*;
import org.apache.solr.ivrixdb.search.stream.stats.TopValuesMetric;
import org.apache.solr.ivrixdb.search.utilities.OffsetIterator;

/**
 * This object is the results storage of a search job.
 * It holds the timeline, the field summaries, the events,
 * and the results. It is updated through the Search Job's
 * push system.
 *
 * @author Ivri Faitelson
 */
public class SearchJobStorage {
  /**
   * Defines the type of tuples being pushed into storage.
   * May be of type events or results (transformed events).
   */
  public enum StorageType {
    EVENTS,
    RESULTS
  }

  private final StorageLocker storageLocker;
  private final LinkedList<Tuple> storedResults;
  private final FieldSummarizer fieldSummarizer;
  private DynamicTimeline timeline;

  /**
   * Constructs the storage with default objects.
   */
  public SearchJobStorage() {
    this.storageLocker = new StorageLocker();
    this.storedResults = new LinkedList<>();
    this.fieldSummarizer = new FieldSummarizer();
    this.timeline = null;
  }

  /**
   * Initializes the objects for a push of tuples.
   * If push is non-stream, getters will be locked
   * for the storage type until finished.
   */
  public void initProcessing(StoragePushProperties pushProperties) {
    if (pushProperties.getPushMethod() == StoragePushProperties.PushMethod.NON_STREAM) {
      StorageType storageType = pushProperties.getStorageType();
      storageLocker.lockWrite(storageType);
    }
    _initProcessing(pushProperties);
  }

  /**
   * processes a tuple from a push.
   */
  public void process(Tuple tuple, StoragePushProperties pushProperties) {
    if (pushProperties.getPushMethod() == StoragePushProperties.PushMethod.NON_STREAM) {
      _process(tuple, pushProperties);

    } else {
      StorageType storageType = pushProperties.getStorageType();
      storageLocker.lockWrite(storageType);
      _process(tuple, pushProperties);
      storageLocker.unlockWrite(storageType);
    }
  }

  /**
   * Finalizes the objects updated by a push.
   * Unlocks getters for storage type if getters
   * have been locked.
   */
  public void finishProcessing(StoragePushProperties pushProperties) {
    StorageType storageType = pushProperties.getStorageType();
    if (storageLocker.isCurrentThreadHoldingWriteLock(storageType)) {
      storageLocker.unlockWrite(storageType);
    }
  }

  /**
   * @return The current timeline.
   */
  public Map<String, Object> getTimelinePresentation() {
    storageLocker.lockRead(StorageType.EVENTS);
    Map<String, Object> timelinePresentation = new LinkedHashMap<>();

    if (timeline == null) {
      timelinePresentation.put("totalEventCount", 0);
      timelinePresentation.put("scope", null);
      timelinePresentation.put("buckets", new LinkedList<>());

    } else {
      List<Object> bucketsPresentation = new LinkedList<>();
      for (TimeBucket bucket : timeline.getBuckets()) {
        Map<String, Object> bucketPresentationMap = new LinkedHashMap<>();
        bucketPresentationMap.put("eventCount", bucket.getEventCount());
        bucketPresentationMap.put("lowerBound", bucket.getLowerBound() != null ? bucket.getLowerBound().toString() : null);
        bucketPresentationMap.put("upperBound", bucket.getUpperBound() != null ? bucket.getUpperBound().toString() : null);
        bucketsPresentation.add(bucketPresentationMap);
      }
      timelinePresentation.put("totalEventCount", timeline.getAllInclusiveBucket().getEventCount());
      timelinePresentation.put("scope", timeline.getScope().name());
      timelinePresentation.put("buckets", bucketsPresentation);
    }

    storageLocker.unlockRead(StorageType.EVENTS);
    return timelinePresentation;
  }

  /**
   * @return The current field summaries.
   */
  public Collection<Object> getFieldSummariesPresentation() {
    storageLocker.lockRead(StorageType.EVENTS);
    List<Object> summariesPresentation = new LinkedList<>();

    for (FieldSummary summary : fieldSummarizer.getSummaries()) {
      Map<String, Object> summaryMap = new LinkedHashMap<>();
      summaryMap.put("fieldName", summary.getFieldName());

      Map<String, Object> topValues = new LinkedHashMap<>();
      for (TopValuesMetric.TopFieldValue topFieldValue : summary.getSummaryTopValues().getTopValues()) {
        Map<String, Object> topValueProperties = new LinkedHashMap<>();
        topValueProperties.put("count", topFieldValue.getCount());
        topValueProperties.put("percentage", topFieldValue.getPercentage());
        topValues.put(topFieldValue.getValue(), topValueProperties);
      }
      summaryMap.put("topValues", topValues);

      for (Metric summaryMetric : summary.getSummaryMetrics()) {
        summaryMap.put(summaryMetric.getFunctionName(), summaryMetric.getValue());
      }
      summariesPresentation.add(summaryMap);
    }

    storageLocker.unlockRead(StorageType.EVENTS);
    return summariesPresentation;
  }

  /**
   * @return The current results at a given page (offset + number of results to return.)
   */
  public Collection<Tuple> getStoredResults(int offset, int size) {
    storageLocker.lockRead(StorageType.RESULTS);
    Iterator<Tuple> storageIterator = storedResults.iterator();
    Collection<Tuple> storedResults = getStoredTuples(storageIterator, offset, size);
    storageLocker.unlockRead(StorageType.RESULTS);
    return storedResults;
  }

  /**
   * @return The current events at a given page (offset + number of events to return.)
   */
  public Collection<Tuple> getStoredEvents(int offset, int size) {
    storageLocker.lockRead(StorageType.EVENTS);
    Collection<Tuple> storedEvents;
    if (timeline == null) {
      storedEvents = new LinkedList<>();

    } else {
      Iterator<Tuple> storageIterator = timeline.getAllInclusiveIterator();
      storedEvents = getStoredTuples(storageIterator, offset, size);
    }
    storageLocker.unlockRead(StorageType.EVENTS);
    return storedEvents;
  }

  /**
   * @return The current events at a given date-time range for a given page (offset + number of events to return.)
   */
  public Collection<Tuple> getStoredEvents(int offset, int size, OffsetDateTime earliest, OffsetDateTime latest) {
    Collection<Tuple> storedEvents;
    storageLocker.lockRead(StorageType.EVENTS);
    if (timeline == null) {
      storedEvents = new LinkedList<>();
    } else {
      Iterator<Tuple> storageIterator = timeline.getSelectedBucketsIterator(earliest, latest);
      storedEvents = getStoredTuples(storageIterator, offset, size);
    }
    storageLocker.unlockRead(StorageType.EVENTS);
    return storedEvents;
  }



  private void _initProcessing(StoragePushProperties properties) {
    if (properties.getStorageType() == StorageType.EVENTS) {
      fieldSummarizer.reset();
      if (timeline != null) {
        timeline.reset();
      } else {
        timeline = new DynamicTimeline(properties.getPushOrder());
      }

    } else if (properties.getStorageType() == StorageType.RESULTS) {
      storedResults.clear();
    }
  }

  private void _process(Tuple tuple, StoragePushProperties properties) {
    if (properties.getStorageType() == StorageType.EVENTS) {
      timeline.digestEvent(tuple, properties.getPushMethod());
      fieldSummarizer.digestEvent(tuple);

    } else if (properties.getStorageType() == StorageType.RESULTS) {
      storedResults.add(tuple);
    }
  }


  private Collection<Tuple> getStoredTuples(Iterator<Tuple> tuplesIterator, int offset, int size) {
    OffsetIterator tuplesOffsetIterator = new OffsetIterator(tuplesIterator, offset);
    Collection<Tuple> tuples = new LinkedList<>();
    while (tuplesOffsetIterator.hasNext()) {
      Tuple tuple = tuplesOffsetIterator.next();
      tuples.add(tuple);
      if (tuples.size() >= size) {
        break;
      }
    }
    return tuples;
  }
}
