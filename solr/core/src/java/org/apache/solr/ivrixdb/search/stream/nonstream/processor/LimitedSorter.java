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

package org.apache.solr.ivrixdb.search.stream.nonstream.processor;

import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.ivrixdb.search.stream.nonstream.NonStreamProcessor;
import org.apache.solr.ivrixdb.search.utilities.ComparableTuple;

/**
 * This processor returns the first
 * N tuples in a given sort order.
 *
 * @author Ivri Faitelson
 */
public class LimitedSorter extends NonStreamProcessor {
  private final TreeSet<ComparableTuple> sortedWrappedTuples;
  private final StreamComparator comparator;
  private final int limit;

  /**
   * @param comparator the field and sort order.
   * @param limit the number of tuples to hold in memory.
   * @param canBePreviewed if the processor can be previewed or not.
   */
  public LimitedSorter(StreamComparator comparator, int limit, boolean canBePreviewed) {
    super(canBePreviewed);
    this.limit = limit;
    this.comparator = comparator;
    this.sortedWrappedTuples = new TreeSet<>();
  }

  /**
   * {@inheritDoc}
   */
  public NonStreamProcessor createNewWithSameConfig() {
    return new LimitedSorter(comparator, limit, lockable);
  }

  /**
   * returns an iterator over sorted tuples.
   */
  public Iterator<Tuple> generatedTuplesIterator() {
    return ComparableTuple.createIteratorForComparableTuples(sortedWrappedTuples);
  }

  protected void _process(Tuple tuple) {
    sortedWrappedTuples.add(new ComparableTuple(tuple, comparator));
    if (sortedWrappedTuples.size() > limit) {
      sortedWrappedTuples.pollLast();
    }
  }
}
