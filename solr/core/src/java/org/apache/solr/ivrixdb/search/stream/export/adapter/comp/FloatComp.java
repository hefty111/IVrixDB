
package org.apache.solr.ivrixdb.search.stream.export.adapter.comp;

public abstract class FloatComp {
  public abstract int compare(float a, float b);
  public abstract float resetValue();

  public static FloatComp asc() {
    return new FloatComp() {
      public float resetValue() {
        return Float.MAX_VALUE;
      }
      public int compare(float a, float b) {
        return Float.compare(b, a);
      }
    };
  }

  public static FloatComp desc() {
    return new FloatComp() {
      public float resetValue() {
        return -Float.MAX_VALUE;
      }
      public int compare(float a, float b) {
        return Float.compare(a, b);
      }
    };
  }
}