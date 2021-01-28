
package org.apache.solr.ivrixdb.search.stream.export.adapter.value;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;
import org.apache.solr.ivrixdb.search.stream.export.adapter.comp.IntComp;

public class IntValue implements SortValue {

  protected NumericDocValues vals;
  protected String field;
  protected int currentValue;
  protected IntComp comp;
  private int lastDocID;
  protected boolean present;

  public Object getCurrentValue() {
    assert present == true;
    return currentValue;
  }

  public String getField() {
    return field;
  }

  public IntValue copy() {
    return new IntValue(field, comp);
  }

  public IntValue(String field, IntComp comp) {
    this.field = field;
    this.comp = comp;
    this.currentValue = comp.resetValue();
    this.present = false;
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.vals = DocValues.getNumeric(context.reader(), field);
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
      currentValue = (int) vals.longValue();
    } else {
      present = false;
      currentValue = 0;
    }
  }

  @Override
  public boolean isPresent() {
    return this.present;
  }

  public int compareTo(SortValue o) {
    IntValue iv = (IntValue)o;
    return comp.compare(currentValue, iv.currentValue);
  }

  public void setCurrentValue(SortValue sv) {
    IntValue iv = (IntValue)sv;
    this.currentValue = iv.currentValue;
    this.present = iv.present;
  }

  public void reset() {
    currentValue = comp.resetValue();
    this.present = false;
  }
}