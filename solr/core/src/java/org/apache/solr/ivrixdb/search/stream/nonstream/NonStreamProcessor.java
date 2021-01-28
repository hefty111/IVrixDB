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

package org.apache.solr.ivrixdb.search.stream.nonstream;

import java.util.*;
import java.util.concurrent.locks.*;

import org.apache.solr.client.solrj.io.Tuple;

/**
 * This class holds the commonalities between non-stream processors.
 * These processors process entire data-sets from a stream,
 * and allow introspection of its generated tuples.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO -- improve the locking. Right now, this blocks the search pipeline until the entire preview pipeline finishes...
 */
public abstract class NonStreamProcessor {
  private final Lock processingLock = new ReentrantLock();
  private boolean finished;
  protected final boolean lockable;

  protected NonStreamProcessor(boolean lockable) {
    this.lockable = lockable;
    this.finished = false;
  }

  /**
   * if lockable, acquires lock, processes tuple, and then releases lock.
   * if not, processes tuple normally.
   */
  public void process(Tuple tuple) {
    if (!lockable) {
      _process(tuple);

    } else {
      processingLock.lock();
      _process(tuple);
      processingLock.unlock();
    }
  }

  /**
   * locks processing and waits for processing lock acquisition.
   * returns immediately if already acquired.
   */
  public void lockProcessing() {
    validateLockable();
    processingLock.lock();
  }

  /**
   * releases the processing lock and returns immediately.
   */
  public void unlockProcessing() {
    validateLockable();
    processingLock.unlock();
  }

  /**
   * sets the processor state to finished.
   * uses intrinsic locking.
   */
  public synchronized void finished() {
    finished = true;
  }

  /**
   * returns the state of the processor.
   * uses intrinsic locking.
   */
  public synchronized boolean isFinished() {
    return finished;
  }

  /**
   * returns a copy of this processor with the same config.
   */
  public abstract NonStreamProcessor createNewWithSameConfig();

  /**
   * returns an iterator for all the in-memory generated tuples.
   * must provide support for hasNext(), next(), and remove().
   */
  public abstract Iterator<Tuple> generatedTuplesIterator();

  protected abstract void _process(Tuple tuple);

  private void validateLockable() {
    if (!lockable) {
      throw new UnsupportedOperationException("NonStreamProcessor is not lockable");
    }
  }
}
