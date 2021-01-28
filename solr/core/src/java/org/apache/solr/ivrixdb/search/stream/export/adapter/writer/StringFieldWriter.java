
package org.apache.solr.ivrixdb.search.stream.export.adapter.writer;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortDoc;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortValue;
import org.apache.solr.ivrixdb.search.stream.export.adapter.FieldWriter;
import org.apache.solr.schema.FieldType;

public class StringFieldWriter extends FieldWriter {
  private String field;
  private FieldType fieldType;
  private CharsRefBuilder cref = new CharsRefBuilder();
  final ByteArrayUtf8CharSequence utf8 = new ByteArrayUtf8CharSequence(new byte[0], 0, 0) {
    @Override
    public String toString() {
      String str = super.utf16;
      if (str != null) return str;
      fieldType.indexedToReadable(new BytesRef(super.buf, super.offset, super.length), cref);
      str = cref.toString();
      super.utf16 = str;
      return str;
    }
  };

  public StringFieldWriter(String field, FieldType fieldType) {
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

    if (ew instanceof JavaBinCodec.BinEntryWriter) {
      ew.put(this.field, utf8.reset(ref.bytes, ref.offset, ref.length, null));
    } else {
      fieldType.indexedToReadable(ref, cref);
      String v = cref.toString();
      ew.put(this.field, v);
    }
    return true;
  }
}