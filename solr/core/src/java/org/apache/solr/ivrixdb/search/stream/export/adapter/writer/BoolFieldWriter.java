
package org.apache.solr.ivrixdb.search.stream.export.adapter.writer;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.ivrixdb.search.stream.export.adapter.FieldWriter;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortDoc;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;
import org.apache.solr.schema.FieldType;

public class BoolFieldWriter extends FieldWriter {
  private String field;
  private FieldType fieldType;
  private CharsRefBuilder cref = new CharsRefBuilder();

  public BoolFieldWriter(String field, FieldType fieldType) {
    this.field = field;
    this.fieldType = fieldType;
  }

  public boolean write(SortDoc sortDoc, LeafReader reader, Map ew, int fieldIndex) throws IOException {
    BytesRef ref;
    SortValue sortValue = sortDoc.getSortValue(this.field);
    if (sortValue != null) {
      if (sortValue.isPresent()) {
        ref = (BytesRef) sortValue.getCurrentValue();
      } else { //empty-value
        return false;
      }
    } else {
      // field is not part of 'sort' param, but part of 'fl' param
      SortedDocValues vals = DocValues.getSorted(reader, this.field);
      if (vals.advance(sortDoc.docId) != sortDoc.docId) {
        return false;
      }
      int ord = vals.ordValue();
      ref = vals.lookupOrd(ord);
    }

    fieldType.indexedToReadable(ref, cref);
    ew.put(this.field, "true".equals(cref.toString()));
    return true;
  }
}
