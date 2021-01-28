
package org.apache.solr.ivrixdb.search.stream.export.adapter.doc;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortDoc;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;

public class QuadValueSortDoc extends TripleValueSortDoc {

  protected SortValue value4;

  public SortValue getSortValue(String field) {
    if (value1.getField().equals(field)) {
      return value1;
    } else if (value2.getField().equals(field)) {
      return value2;
    } else if (value3.getField().equals(field)) {
      return value3;
    } else if (value4.getField().equals(field)) {
      return value4;
    }
    return null;
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.ord = context.ord;
    this.docBase = context.docBase;
    value1.setNextReader(context);
    value2.setNextReader(context);
    value3.setNextReader(context);
    value4.setNextReader(context);
  }

  public void reset() {
    this.docId = -1;
    this.docBase = -1;
    value1.reset();
    value2.reset();
    value3.reset();
    value4.reset();
  }

  public void setValues(int docId) throws IOException {
    this.docId = docId;
    value1.setCurrentValue(docId);
    value2.setCurrentValue(docId);
    value3.setCurrentValue(docId);
    value4.setCurrentValue(docId);
  }

  public void setValues(SortDoc sortDoc) {
    this.docId = sortDoc.docId;
    this.ord = sortDoc.ord;
    this.docBase = sortDoc.docBase;
    value1.setCurrentValue(((QuadValueSortDoc)sortDoc).value1);
    value2.setCurrentValue(((QuadValueSortDoc)sortDoc).value2);
    value3.setCurrentValue(((QuadValueSortDoc)sortDoc).value3);
    value4.setCurrentValue(((QuadValueSortDoc)sortDoc).value4);
  }

  public QuadValueSortDoc(SortValue value1, SortValue value2, SortValue value3, SortValue value4) {
    super(value1, value2, value3);
    this.value4 = value4;
  }

  public SortDoc copy() {
    return new QuadValueSortDoc(value1.copy(), value2.copy(), value3.copy(), value4.copy());
  }

  public boolean lessThan(Object o) {

    QuadValueSortDoc sd = (QuadValueSortDoc)o;
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
        comp = value3.compareTo(sd.value3);
        if(comp == -1) {
          return true;
        } else if (comp == 1) {
          return false;
        } else {
          comp = value4.compareTo(sd.value4);
          if(comp == -1) {
            return true;
          } else if (comp == 1) {
            return false;
          } else {
            return docId+docBase > sd.docId+sd.docBase;
          }
        }
      }
    }
  }

  public int compareTo(Object o) {
    QuadValueSortDoc sd = (QuadValueSortDoc)o;
    int comp = value1.compareTo(sd.value1);
    if(comp == 0) {
      comp = value2.compareTo(sd.value2);
      if(comp == 0) {
        comp = value3.compareTo(sd.value3);
        if(comp == 0) {
          return value4.compareTo(sd.value4);
        } else {
          return comp;
        }
      } else {
        return comp;
      }
    } else {
      return comp;
    }
  }
}