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

package org.apache.solr.ivrixdb.core.overseer.response;

import java.util.*;

import org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket;

/**
 * This object holds and organizes IVrix Bucket names in the way that IVrixDB must search them;
 * First all the HOT buckets in reverse-time-order, then all the WARM buckets in reverse-time-order,
 * and then lastly all the COLD buckets in reverse-time-order.
 *
 * @author Ivri Faitelson
 */
public class OrganizedBucketsForSearch extends LinkedList<String> {
  /**
   * @param bucketsList A list of IVrix Buckets to organize for search
   * @return A list of bucket names are organized in the way that IVrixDB must search them
   */
  public static OrganizedBucketsForSearch fromBucketsList(List<IVrixBucket> bucketsList) {
    bucketsList.sort(new BucketStateAndReverseTimeBoundsComparator());

    OrganizedBucketsForSearch organizedBuckets = new OrganizedBucketsForSearch();
    for (IVrixBucket bucket : bucketsList) {
      organizedBuckets.add(bucket.getName());
    }
    return organizedBuckets;
  }

  /**
   * @return A list of organized bucket names that were sent by the IVrix Overseer
   */
  public static OrganizedBucketsForSearch fromOverseerResponse(Object overseerResponse) {
    OrganizedBucketsForSearch organizedBuckets = new OrganizedBucketsForSearch();
    organizedBuckets.addAll((List<String>)overseerResponse);
    return organizedBuckets;
  }

  private static class BucketStateAndReverseTimeBoundsComparator implements Comparator<IVrixBucket> {
    @Override
    public int compare(IVrixBucket _this, IVrixBucket _that) {
      if (_this.getBucketType().ordinal() != _that.getBucketType().ordinal()) {
        return _this.getBucketType().ordinal() < _that.getBucketType().ordinal() ? -1 : 1;

      } else {
        if (!_this.getTimeBounds().isBounded() || !_that.getTimeBounds().isBounded()) {
          return 0;
        }
        if (_this.getTimeBounds().getLongRepresentation() > _that.getTimeBounds().getLongRepresentation()) {
          return -1;
        } else {
          return 1;
        }
      }
    }
  }
}
