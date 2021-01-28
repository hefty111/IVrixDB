
package org.apache.solr.ivrixdb.search.stream.export.adapter.doc;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortDoc;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;

public class DoubleValueSortDoc extends SingleValueSortDoc {

  protected SortValue value2;

  public SortValue getSortValue(String field) {
    if (value1.getField().equals(field)) {
      return value1;
    } else if (value2.getField().equals(field)) {
      return value2;
    }
    return null;
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.ord = context.ord;
    this.docBase = context.docBase;
    value1.setNextReader(context);
    value2.setNextReader(context);
  }

  public void reset() {
    this.docId = -1;
    this.docBase = -1;
    value1.reset();
    value2.reset();
  }

  public void setValues(int docId) throws IOException {
    this.docId = docId;
    value1.setCurrentValue(docId);
    value2.setCurrentValue(docId);
  }

  public void setValues(SortDoc sortDoc) {
    this.docId = sortDoc.docId;
    this.ord = sortDoc.ord;
    this.docBase = sortDoc.docBase;
    value1.setCurrentValue(((DoubleValueSortDoc)sortDoc).value1);
    value2.setCurrentValue(((DoubleValueSortDoc)sortDoc).value2);
  }

  public DoubleValueSortDoc(SortValue value1, SortValue value2) {
    super(value1);
    this.value2 = value2;
  }

  public SortDoc copy() {
    return new DoubleValueSortDoc(value1.copy(), value2.copy());
  }

  public boolean lessThan(Object o) {
    DoubleValueSortDoc sd = (DoubleValueSortDoc)o;
    int comp = value1.compareTo(sd.value1);
    if(comp == -1) {
      return true;
    } else if (comp == 1) {
      return false;
    } else {
      comp = value2.compareTo(sd.value2);
      if(comp == -1) {
        return true;
      } else if (comp == 1) {
        return false;
      } else {
        return docId+docBase > sd.docId+sd.docBase;
      }
    }
  }

  public int compareTo(Object o) {
    DoubleValueSortDoc sd = (DoubleValueSortDoc)o;
    int comp = value1.compareTo(sd.value1);
    if (comp == 0) {
      return value2.compareTo(sd.value2);
    } else {
      return comp;
    }
  }
}