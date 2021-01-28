
package org.apache.solr.ivrixdb.search.stream.export.adapter.comp;

public abstract class DoubleComp {
  public abstract int compare(double a, double b);
  public abstract double resetValue();

  public static DoubleComp asc() {
    return new DoubleComp() {
      public double resetValue() {
        return Double.MAX_VALUE;
      }
      public int compare(double a, double b) {
        return Double.compare(b, a);
      }
    };
  }

  public static DoubleComp desc() {
    return new DoubleComp() {
      public double resetValue() {
        return -Double.MAX_VALUE;
      }
      public int compare(double a, double b) {
        return Double.compare(a, b);
      }
    };
  }
}
