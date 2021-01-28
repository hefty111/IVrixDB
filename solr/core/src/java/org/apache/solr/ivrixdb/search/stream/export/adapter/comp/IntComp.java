
package org.apache.solr.ivrixdb.search.stream.export.adapter.comp;

public abstract class IntComp {
  public abstract int compare(int a, int b);
  public abstract int resetValue();

  public static IntComp asc() {
    return new IntComp() {
      public int resetValue() {
        return Integer.MAX_VALUE;
      }
      public int compare(int a, int b) {
        return Integer.compare(b, a);
      }
    };
  }

  public static IntComp desc() {
    return new IntComp() {
      public int resetValue() {
        return Integer.MIN_VALUE;
      }
      public int compare(int a, int b) {
        return Integer.compare(a, b);
      }
    };
  }
}
