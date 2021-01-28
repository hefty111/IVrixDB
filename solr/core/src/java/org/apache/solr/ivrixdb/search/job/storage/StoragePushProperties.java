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

import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.ivrixdb.search.job.storage.SearchJobStorage.StorageType;

/**
 * This class holds the properties of a push of
 * tuples from a pipeline to the Search Job.
 *
 * @author Ivri Faitelson
 */
public class StoragePushProperties {
  /**
   * Defines the way the tuples are being pushed to the search job.
   * Can either be from a streaming or a non-streaming pipeline segment.
   */
  public enum PushMethod {
    NON_STREAM,
    STREAM
  }

  private final StreamComparator pushOrder;
  private final StorageType storageType;
  private final PushMethod pushMethod;

  /**
   * @param pushOrder The sort order of the tuples being pushed.
   * @param storageType The type of storage the tuples belong to.
   * @param pushMethod The way the tuples are being pushed to the search job.
   */
  public StoragePushProperties(StreamComparator pushOrder, StorageType storageType, PushMethod pushMethod) {
    this.pushOrder = pushOrder;
    this.storageType = storageType;
    this.pushMethod = pushMethod;
  }

  /**
   * @return The way the tuples are being pushed to the Search Job.
   */
  public PushMethod getPushMethod() {
    return pushMethod;
  }

  /**
   * @return The type of storage the tuples belong to.
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * @return The sort order of the tuples being pushed.
   */
  public StreamComparator getPushOrder() {
    return pushOrder;
  }
}
