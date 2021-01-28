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

/**
 * This object behaves like a runtime timer
 * for the executors of the search job.
 *
 * @author Ivri Faitelson
 */
public class ExecutionDuration {
  private long startTime;
  private long duration;

  /**
   * Constructs with default values.
   */
  public ExecutionDuration() {
    this.duration = 0;
    this.startTime = 0;
  }

  /**
   * Begins counting.
   */
  public void start() {
    startTime = System.nanoTime();
  }

  /**
   * finalizes the duration.
   */
  public void finish() {
    duration = System.nanoTime() - startTime;
  }

  /**
   * @return The current (or finalized) duration of execution.
   */
  public long getDurationLength() {
    long _duration;
    if (duration == 0) {
      _duration = System.nanoTime() - startTime;
    } else {
      _duration = duration;
    }
    return _duration;
  }
}
