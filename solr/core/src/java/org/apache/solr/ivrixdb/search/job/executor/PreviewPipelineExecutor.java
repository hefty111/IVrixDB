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
import java.util.List;

import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.ivrixdb.search.job.SearchJob;
import org.apache.solr.ivrixdb.search.stream.nonstream.NonStreamPreviewStream;
import org.slf4j.*;


/**
 * This executor runs the preview pipeline at intervals.
 *
 * @author Ivri Faitelson
 */
public class PreviewPipelineExecutor implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int DEFAULT_INTERVAL_MILLISECONDS = 2000;

  private final TupleStream previewStream;
  private final SearchJob searchJob;
  private final ExecutionDuration duration;
  private final Thread thread;

  /**
   * @param previewStream The preview pipeline to execute.
   * @param searchJob The search job the preview pipeline belongs to.
   */
  public PreviewPipelineExecutor(TupleStream previewStream, SearchJob searchJob) {
    this.previewStream = previewStream;
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
   * Interrupts the execution thread.
   */
  public void cancel() {
    thread.interrupt();
    duration.finish();
  }

  /**
   * @return The duration of the preview execution.
   */
  public long getExecutionDuration() {
    return duration.getDurationLength();
  }


  /**
   * The target of the execution thread.
   */
  public void run() {
    while(canProvidePreview()) {
      runPreview();
      sleepThread();
    }
    duration.finish();
  }

  private void runPreview() {
    try {
      previewStream.open();
      while (!previewStream.read().EOF) {}

    } catch (Exception e) {
      log.error("Search Job's Preview Executor --  " + searchJob.getJobID() + " -- failed", e);
      e.printStackTrace();

    } finally {
      try {
        previewStream.close();
      } catch (Exception e) {
        log.error("Search Job's Preview Executor --  " + searchJob.getJobID() + " -- failed to shutdown", e);
        e.printStackTrace();
      }
    }
  }

  private void sleepThread() {
    try {
      Thread.sleep(DEFAULT_INTERVAL_MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * returns if the pipeline can run. The pipeline can only run
   * when the first non-streaming decorator has not be finished.
   */
  private boolean canProvidePreview() {
    return canProvidePreview(previewStream.children());
  }

  private boolean canProvidePreview(List<TupleStream> children) {
    boolean canProvidePreview = false;
    if (children.size() > 0) {
      TupleStream child = children.get(0);

      if (child instanceof NonStreamPreviewStream) {
        canProvidePreview = !((NonStreamPreviewStream)child).isSourceProcessorFinished();
      } else {
        canProvidePreview = canProvidePreview(child.children());
      }
    }
    return canProvidePreview;
  }
}