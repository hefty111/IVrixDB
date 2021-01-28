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

package org.apache.solr.ivrixdb.core.overseer.state;

/**
 * This object defines an IVrix Bucket holder.
 * These holders are for ensuring that a Bucket
 * does not get detached while a Search Job
 * is actively streaming from it. A Bucket Holder
 * is defined by the Search Job and the IVrix Node
 * where that Search Job is running.
 *
 * @author Ivri Faitelson
 */
public class BucketHolder {
  public final String nodeName;
  public final String searchJobID;

  /**
   * @param searchJobID The ID of the Search Job
   * @param nodeName The name of the IVrix Node where the Search Job is running
   */
  public BucketHolder(String searchJobID, String nodeName) {
    this.nodeName = nodeName;
    this.searchJobID = searchJobID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BucketHolder that = (BucketHolder)o;
    return nodeName.equals(that.nodeName) && searchJobID.equals(that.searchJobID);
  }
}
