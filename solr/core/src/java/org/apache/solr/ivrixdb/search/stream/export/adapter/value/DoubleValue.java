
package org.apache.solr.ivrixdb.search.stream.export.adapter.value;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;
import org.apache.solr.ivrixdb.search.stream.export.adapter.comp.DoubleComp;

public class DoubleValue implements SortValue {

  protected NumericDocValues vals;
  protected String field;
  protected double currentValue;
  protected DoubleComp comp;
  private int lastDocID;
  private LeafReader reader;
  private boolean present;

  public DoubleValue(String field, DoubleComp comp) {
    this.field = field;
    this.comp = comp;
    this.currentValue = comp.resetValue();
    this.present = false;
  }

  public Object getCurrentValue() {
    assert present == true;
    return currentValue;
  }

  public String getField() {
    return field;
  }

  public DoubleValue copy() {
    return new DoubleValue(field, comp);
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.reader = context.reader();
    this.vals = DocValues.getNumeric(this.reader, this.field);
    lastDocID = 0;
  }

  public void setCurrentValue(int docId) throws IOException {
    if (docId < lastDocID) {
      throw new AssertionError("docs were sent out-of-order: lastDocID=" + lastDocID + " vs doc=" + docId);
    }
    lastDocID = docId;
    int curDocID = vals.docID();
    if (docId > curDocID) {
      curDocID = vals.advance(docId);
    }
    if (docId == curDocID) {
      present = true;
      currentValue = Double.longBitsToDouble(vals.longValue());
    } else {
      present = false;
      currentValue = 0f;
    }
  }

  @Override
  public boolean isPresent() {
    return present;
  }

  public void setCurrentValue(SortValue sv) {
    DoubleValue dv = (DoubleValue)sv;
    this.currentValue = dv.currentValue;
    this.present = dv.present;
  }

  public void reset() {
    this.currentValue = comp.resetValue();
    this.present = false;
  }

  public int compareTo(SortValue o) {
    DoubleValue dv = (DoubleValue)o;
    return comp.compare(currentValue, dv.currentValue);
  }
}
