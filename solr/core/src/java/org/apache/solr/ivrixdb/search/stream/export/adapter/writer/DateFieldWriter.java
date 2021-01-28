
package org.apache.solr.ivrixdb.search.stream.export.adapter.writer;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortDoc;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;
import org.apache.solr.ivrixdb.search.stream.export.adapter.FieldWriter;

public class DateFieldWriter extends FieldWriter {
  private String field;

  public DateFieldWriter(String field) {
    this.field = field;
  }

  public boolean write(SortDoc sortDoc, LeafReader reader, Map ew, int fieldIndex) throws IOException {
    Long val;
    SortValue sortValue = sortDoc.getSortValue(this.field);
    if (sortValue != null) {
      if (sortValue.isPresent()) {
        val = (long) sortValue.getCurrentValue();
      } else { //empty-value
        return false;
      }
    } else {
      // field is not part of 'sort' param, but part of 'fl' param
      NumericDocValues vals = DocValues.getNumeric(reader, this.field);
      if (vals.advance(sortDoc.docId) == sortDoc.docId) {
        val = vals.longValue();
      } else {
        return false;
      }
    }
    ew.put(this.field, new Date(val));
    return true;
  }
}
