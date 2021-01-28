
package org.apache.solr.ivrixdb.search.stream.export.adapter.comp;

public abstract class LongComp {
  public abstract int compare(long a, long b);
  public abstract long resetValue();

  public static LongComp asc() {
    return new LongComp() {
      public long resetValue() {
        return Long.MAX_VALUE;
      }
      public int compare(long a, long b) {
        return Long.compare(b, a);
      }
    };
  }

  public static LongComp desc() {
    return new LongComp() {
      public long resetValue() {
        return Long.MIN_VALUE;
      }
      public int compare(long a, long b) {
        return Long.compare(a, b);
      }
    };
  }
}
