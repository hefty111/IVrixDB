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

package org.apache.solr.ivrixdb.search.job.storage.timeline;

import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;

/**
 * This iterator enables iteration over several time buckets.
 * holds the iterators of the time buckets in a TreeSet,
 * and calls next() on the right one given a Comparator.
 *
 * @author Ivri Faitelson
 */
public class CombinedBucketsIterator implements Iterator<Tuple> {
  private final TreeSet<BucketIterator> bucketIterators;
  private BucketIterator currentIterator;

  /**
   * @param buckets The buckets to grab iterators from.
   * @param comparator The Comparator used to determine the next iterator to use.
   */
  public CombinedBucketsIterator(Collection<TimeBucket> buckets, Comparator<Tuple> comparator) {
    this.bucketIterators = new TreeSet<>();

    for (TimeBucket bucket : buckets) {
      BucketIterator bucketIterator = new BucketIterator(bucket.getStoredEventsIterator(), comparator);
      if (bucketIterator.next()) {
        bucketIterators.add(bucketIterator);
      }
    }
    currentIterator = bucketIterators.pollFirst();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasNext() {
    return currentIterator != null && currentIterator.hasNext();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple next() {
    Tuple next = null;
    if (currentIterator != null) {
      next = currentIterator.getTuple();
      if (currentIterator.next()) {
        bucketIterators.add(currentIterator);
      }
      currentIterator = bucketIterators.pollFirst();
    }
    return next;
  }


  private static class BucketIterator implements Comparable<BucketIterator> {
    private Tuple tuple;
    private Comparator<Tuple> comparator;
    private Iterator<Tuple> eventsIterator;

    public BucketIterator(Iterator<Tuple> eventsIterator, Comparator<Tuple> comparator) {
      this.eventsIterator = eventsIterator;
      this.comparator = comparator;
    }

    public Tuple getTuple() {
      return tuple;
    }

    public boolean hasNext() {
      return tuple != null;
    }

    public boolean next() {
      boolean hasNext = eventsIterator.hasNext();
      if (hasNext) {
        tuple = eventsIterator.next();
      } else {
        tuple = null;
      }

      return hasNext;
    }

    public int compareTo(BucketIterator that) {
      if(this == that) {
        return 0;
      }

      int i = comparator.compare(this.tuple, that.tuple);
      if(i == 0) {
        return 1;
      } else {
        return i;
      }
    }

    public boolean equals(Object o) {
      return this == o;
    }
  }
}
