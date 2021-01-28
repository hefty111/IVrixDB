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

import java.util.Iterator;

import org.apache.solr.client.solrj.io.Tuple;

/**
 * This iterator wraps a separate iterator, and
 * places its cursor at a given offset.
 *
 * @author Ivri Faitelson
 */
public class OffsetIterator implements Iterator<Tuple> {
  private final Iterator<Tuple> wrappedIterator;

  /**
   * @param wrappedIterator The iterator to wrap.
   * @param offset The offset where the wrapper iterator's cursor will be moved to.
   */
  public OffsetIterator(Iterator<Tuple> wrappedIterator, int offset) {
    this.wrappedIterator = wrappedIterator;
    attemptToReachOffset(offset);
  }

  private void attemptToReachOffset(int offset) {
    int position = 0;
    while (position < offset - 1) {
      if (this.hasNext()) {
        this.next();
        position += 1;
      } else {
        break;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasNext() {
    return wrappedIterator.hasNext();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple next() {
    return wrappedIterator.next();
  }
}
