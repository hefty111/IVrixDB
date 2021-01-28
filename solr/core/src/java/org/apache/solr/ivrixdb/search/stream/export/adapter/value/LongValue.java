
package org.apache.solr.ivrixdb.search.stream.export.adapter.value;

import java.io.IOException;
import org.apache.lucene.index.*;
import org.apache.solr.ivrixdb.search.stream.export.adapter.comp.LongComp;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;

public class LongValue implements SortValue {

  protected NumericDocValues vals;
  protected String field;
  protected long currentValue;
  public LongComp comp;
  private int lastDocID;
  private boolean present;

  public LongValue(String field, LongComp comp) {
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

  public LongValue copy() {
    return new LongValue(field, comp);
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
      currentValue = vals.longValue();
    } else {
      present = false;
      currentValue = 0;
    }
  }

  @Override
  public boolean isPresent() {
    return present;
  }

  public void setCurrentValue(SortValue sv) {
    LongValue lv = (LongValue)sv;
    this.currentValue = lv.currentValue;
    this.present = lv.present;
  }

  public int compareTo(SortValue o) {
    LongValue l = (LongValue)o;
    return comp.compare(currentValue, l.currentValue);
  }

  public void reset() {
    this.currentValue = comp.resetValue();
    this.present = false;
  }
}
