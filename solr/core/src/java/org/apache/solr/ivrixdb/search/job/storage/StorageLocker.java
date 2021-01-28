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

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.solr.ivrixdb.search.job.storage.SearchJobStorage.StorageType;

/**
 * This class holds the storage lockers for each storage type
 * {@link org.apache.solr.ivrixdb.search.job.storage.SearchJobStorage.StorageType}.
 *
 * @author Ivri Faitelson
 */
public class StorageLocker {
  private final ReentrantReadWriteLock eventsLock;
  private final ReentrantReadWriteLock resultsLock;

  /**
   * Constructs with default locks for each storage type.
   */
  public StorageLocker() {
    this.eventsLock = new ReentrantReadWriteLock();
    this.resultsLock = new ReentrantReadWriteLock();
  }

  /**
   * Locks the write lock of a storage type.
   */
  public void lockWrite(StorageType storageType) {
    getReadWriteLock(storageType).writeLock().lock();
  }

  /**
   * Unlocks the write lock of a storage type.
   */
  public void unlockWrite(StorageType storageType) {
    getReadWriteLock(storageType).writeLock().unlock();
  }

  /**
   * Locks the read lock of a storage type.
   */
  public void lockRead(StorageType storageType) {
    getReadWriteLock(storageType).readLock().lock();
  }

  /**
   * Unlocks the read lock of a storage type.
   */
  public void unlockRead(StorageType storageType) {
    getReadWriteLock(storageType).readLock().unlock();
  }

  /**
   * @return Is the current thread holding the write lock of a storage type.
   */
  public boolean isCurrentThreadHoldingWriteLock(StorageType storageType) {
    return getReadWriteLock(storageType).writeLock().isHeldByCurrentThread();
  }

  private ReentrantReadWriteLock getReadWriteLock(StorageType storageType) {
    if (storageType == StorageType.EVENTS) {
      return eventsLock;
    } else {
      return resultsLock;
    }
  }
}
