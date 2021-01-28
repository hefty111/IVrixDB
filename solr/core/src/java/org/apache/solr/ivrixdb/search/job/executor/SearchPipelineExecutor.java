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

package org.apache.solr.ivrixdb.search.job.executor;

import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.ivrixdb.search.job.SearchJob;
import org.slf4j.*;

/**
 * This executor will run the search pipeline once,
 * and signal to the Search Job when it completed.
 *
 * @author Ivri Faitelson
 */
public class SearchPipelineExecutor implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ExecutionDuration duration;
  private final TupleStream mainPipeline;
  private final SearchJob searchJob;
  private final Thread thread;

  /**
   * @param mainPipeline The search pipeline to execute.
   * @param searchJob The Search Job the search pipeline belongs to.
   */
  public SearchPipelineExecutor(TupleStream mainPipeline, SearchJob searchJob) {
    this.mainPipeline = mainPipeline;
    this.searchJob = searchJob;
    this.thread = new Thread(this, this.getClass().getName());
    this.duration = new ExecutionDuration();
  }

  /**
   * Begins the execution thread.
   */
  public void start() {
    thread.start();
    duration.start();
  }

  /**
   * @return The duration of the search execution.
   */
  public long getExecutionDuration() {
    return duration.getDurationLength();
  }

  /**
   * The target of the execution thread.
   */
  public void run() {
    try {
      mainPipeline.open();
      while (!mainPipeline.read().EOF) {}

      searchJob.receiveSignalForSearchCompletion();
      duration.finish();

    } catch (Exception e) {
      log.error("Search Job's Search Executor --  " + searchJob.getJobID() + " -- failed", e);
      e.printStackTrace();

    } finally {
      try {
        mainPipeline.close();
      } catch (Exception e) {
        log.error("Search Job's Search Executor --  " + searchJob.getJobID() + " -- failed to shutdown", e);
        e.printStackTrace();
      }
    }
  }
}