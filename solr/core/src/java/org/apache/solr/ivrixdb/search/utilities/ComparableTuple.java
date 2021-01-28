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

package org.apache.solr.ivrixdb.search.utilities;

import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;

/**
 * This object extends off of Solr's Streaming API Tuple object
 * and enables it use the compareTo() function, given a comparator.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO -- I don't remember why I added the equals() function...must figure out if it is necessary
 */
public class ComparableTuple extends Tuple implements Comparable<ComparableTuple> {
  private final Comparator<Tuple> comparator;

  public static Iterator<Tuple> createIteratorForComparableTuples(Collection<ComparableTuple> comparableTupleCollection) {
    return new Iterator<>() {
      private final Iterator<ComparableTuple> wrappersIterator = comparableTupleCollection.iterator();
      @Override
      public boolean hasNext() {
        return wrappersIterator.hasNext();
      }
      @Override
      public Tuple next() {
        return wrappersIterator.next();
      }
      @Override
      public void remove() {
        wrappersIterator.remove();
      }
    };
  }

  /**
   * @param original The original Tuple to wrap
   * @param comparator The comparator that will be used during Tuple comparisons
   */
  public ComparableTuple(Tuple original, Comparator<Tuple> comparator) {
    this.EOF = original.EOF;
    this.EXCEPTION = original.EXCEPTION;
    this.fields = original.fields;
    this.fieldNames = original.fieldNames;
    this.fieldLabels = original.fieldLabels;
    this.comparator = comparator;
  }

  /**
   * @return the result of the comparator function given this Tuple and that Tuple
   */
  public int compareTo(ComparableTuple that) {
    if (this == that) {
      return 0;
    }
    int c = comparator.compare(this, that);
    if (c == 0) {
      return 1;
    } else {
      return c;
    }
  }

  public boolean equals(Object o) {
    return this == o;
  }
}