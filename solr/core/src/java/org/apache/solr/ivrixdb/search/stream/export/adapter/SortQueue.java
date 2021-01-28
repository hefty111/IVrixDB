
package org.apache.solr.ivrixdb.search.stream.export.adapter;


/**
 * populates a Priority Queue with SortDocs
 */
public class SortQueue extends PriorityQueue<SortDoc> {

  private SortDoc proto;
  private Object[] cache;

  public SortQueue(int len, SortDoc proto) {
    super(len);
    this.proto = proto;
  }

  public boolean lessThan(SortDoc t1, SortDoc t2) {
    return t1.lessThan(t2);
  }

  public void populate() {
    Object[] heap = getHeapArray();
    cache = new SortDoc[heap.length];
    for (int i = 1; i < heap.length; i++) {
      cache[i] = heap[i] = proto.copy();
    }
    size = maxSize;
  }

  public void reset() {
    Object[] heap = getHeapArray();
    if(cache != null) {
      System.arraycopy(cache, 1, heap, 1, heap.length-1);
      size = maxSize;
    } else {
      populate();
    }
  }
}