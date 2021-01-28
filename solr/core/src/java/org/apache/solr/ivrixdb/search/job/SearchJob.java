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

package org.apache.solr.ivrixdb.search.job;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.OffsetDateTime;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.TimeBounds;
import org.apache.solr.ivrixdb.search.job.executor.*;
import org.apache.solr.ivrixdb.search.job.storage.*;
import org.apache.solr.ivrixdb.search.utilities.*;
import org.apache.solr.ivrixdb.utilities.Constants;
import org.slf4j.*;

/**
 * A Search Job represents a search that the user requested,
 * and will hold the resulting objects of the search. It
 * will create two separate pipelines, one for the actual search
 * and one to create snapshots of the search pipeline's progress.
 * Each pipeline will have its own executor, with the search executing
 * only once and the preview executing at intervals until the search
 * is complete. The pipelines will send signals and tuples to the
 * search job for updating its storage through a push system.
 * The push system works by having specific components inside the pipeline
 * send tuples one by one to the search job.
 *
 * Search Jobs are stored in memory so that they can be accessed at any time.
 *
 * @author Ivri Faitelson
 */
public class SearchJob {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String jobID;
  private final PreviewPipelineExecutor previewExecutor;
  private final SearchPipelineExecutor searchExecutor;
  private final SearchJobStorage jobStorage;

  private int hitCount;
  private boolean finished;

  /**
   * @param expression The main stream expression that will be modified for both pipelines.
   * @param streamFactory Solr's stream factory for both pipelines.
   * @param context The stream context for both pipelines.
   * @throws IOException If the search job fails to create the pipelines.
   */
  public SearchJob(StreamExpression expression, TimeBounds searchRange,
                   StreamFactory streamFactory, StreamContext context) throws IOException {
    this.jobID = UUID.randomUUID().toString();
    this.jobStorage = new SearchJobStorage();
    this.finished = false;
    this.hitCount = 0;

    TupleStream searchStream = StreamBuilder.buildSearchStream(expression, streamFactory);
    TupleStream previewStream = StreamBuilder.buildPreviewStream(expression, streamFactory);
    context.put(Constants.IVrix.SEARCH_RANGE_IN_CONTEXT, searchRange);
    context.put(Constants.IVrix.SEARCH_JOB_IN_CONTEXT, this);

    if (previewStream != null) {
      StreamBuilder.bridgePreviewWithMain(searchStream, previewStream);
      previewStream.setStreamContext(context);
      this.previewExecutor = new PreviewPipelineExecutor(previewStream, this);
    } else {
      this.previewExecutor = null;
    }

    searchStream.setStreamContext(context);
    this.searchExecutor = new SearchPipelineExecutor(searchStream, this);
  }

  /**
   * Starts the search executor (and the preview executor if necessary).
   */
  public void start() {
    log.info("Starting Search Job -- " + jobID + " -- ...");

    searchExecutor.start();
    if (previewExecutor != null) {
      previewExecutor.start();
    }
  }

  /**
   * Receives a "search has completed" signal from the search executor.
   */
  public void receiveSignalForSearchCompletion() {
    finished = true;
    if (previewExecutor != null) {
      previewExecutor.cancel();
    }

    int totalEventCount = (int)jobStorage.getTimelinePresentation().get("totalEventCount");
    log.info("Finished Search Job -- " + jobID + ". Event Count -- " + totalEventCount);
  }

  /**
   * Receives a "an event has been found" signal from the search pipeline.
   */
  public void receiveSignalForEventHit() {
    hitCount += 1;
  }

  /**
   * Receives a "push is about to begin" signal from one of the pipelines.
   */
  public void receiveSignalForPushInit(StoragePushProperties pushProperties) {
    jobStorage.initProcessing(pushProperties);
  }

  /**
   * Receives a tuple from one of the pipelines to process and maybe store.
   */
  public void storeTuple(Tuple tuple, StoragePushProperties pushProperties) {
    jobStorage.process(tuple, pushProperties);
  }

  /**
   * Receives a "push has finished" signal from one of the pipelines.
   */
  public void receiveSignalForPushFinish(StoragePushProperties pushProperties) {
    jobStorage.finishProcessing(pushProperties);
  }

  /**
   * @return The current events at a given date-time range for a given page (offset + number of events to return.)
   */
  public Collection<Tuple> getStoredEvents(int offset, int size, OffsetDateTime earliest, OffsetDateTime latest) {
    return jobStorage.getStoredEvents(offset, size, earliest, latest);
  }

  /**
   * @return The current events at a given page (offset + number of events to return.)
   */
  public Collection<Tuple> getStoredEvents(int offset, int size) {
    return jobStorage.getStoredEvents(offset, size);
  }

  /**
   * @return The current results at a given page (offset + number of results to return.)
   */
  public Collection<Tuple> getStoredResults(int offset, int size) {
    return jobStorage.getStoredResults(offset, size);
  }

  /**
   * @return The current timeline.
   */
  public Map<String, Object> getTimelinePresentation() {
    return jobStorage.getTimelinePresentation();
  }

  /**
   * @return The current field summaries.
   */
  public Collection<Object> getFieldSummariesPresentation() {
    return jobStorage.getFieldSummariesPresentation();
  }

  /**
   * @return The current execution duration of the search.
   */
  public long getSearchDuration() {
    return searchExecutor.getExecutionDuration();
  }

  /**
   * @return The current number of events were found.
   */
  public int getHitCount() {
    return hitCount;
  }

  /**
   * @return If the search job is finished.
   */
  public boolean isFinished() {
    return finished;
  }

  /**
   * @return The ID of the search job.
   */
  public String getJobID() {
    return jobID;
  }
}