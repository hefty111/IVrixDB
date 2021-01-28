
package org.apache.solr.ivrixdb.search.stream.export.adapter.writer;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.ivrixdb.search.stream.export.adapter.FieldWriter;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortDoc;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;


public class DoubleFieldWriter extends FieldWriter {
  private String field;

  public DoubleFieldWriter(String field) {
    this.field = field;
  }

  public boolean write(SortDoc sortDoc, LeafReader reader, Map ew, int fieldIndex) throws IOException {
    SortValue sortValue = sortDoc.getSortValue(this.field);
    if (sortValue != null) {
      if (sortValue.isPresent()) {
        double val = (double) sortValue.getCurrentValue();
        ew.put(this.field, val);
        return true;
      } else { //empty-value
        return false;
      }
    } else {
      // field is not part of 'sort' param, but part of 'fl' param
      NumericDocValues vals = DocValues.getNumeric(reader, this.field);
      if (vals.advance(sortDoc.docId) == sortDoc.docId) {
        long val = vals.longValue();
        ew.put(this.field, Double.longBitsToDouble(val));
        return true;
      } else {
        return false;
      }
    }
  }
}