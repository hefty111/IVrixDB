
package org.apache.solr.ivrixdb.search.stream.export.adapter.writer;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.function.LongFunction;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.ivrixdb.search.stream.export.adapter.FieldWriter;
import org.apache.solr.ivrixdb.search.stream.export.adapter.SortDoc;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

public class MultiFieldWriter extends FieldWriter {
  private String field;
  private FieldType fieldType;
  private SchemaField schemaField;
  private boolean numeric;
  private CharsRefBuilder cref = new CharsRefBuilder();
  private final LongFunction<Object> bitsToValue;

  public MultiFieldWriter(String field, FieldType fieldType, SchemaField schemaField, boolean numeric) {
    this.field = field;
    this.fieldType = fieldType;
    this.schemaField = schemaField;
    this.numeric = numeric;
    if (this.fieldType.isPointField()) {
      bitsToValue = bitsToValue(fieldType);
    } else {
      bitsToValue = null;
    }
  }

  public boolean write(SortDoc sortDoc, LeafReader reader, Map out, int fieldIndex) throws IOException {
    if (this.fieldType.isPointField()) {
      SortedNumericDocValues vals = DocValues.getSortedNumeric(reader, this.field);
      if (!vals.advanceExact(sortDoc.docId)) return false;
      out.put(this.field,
          (IteratorWriter) w -> {
            for (int i = 0, count = vals.docValueCount(); i < count; i++) {
              w.add(bitsToValue.apply(vals.nextValue()));
            }
          });
      return true;
    } else {
      SortedSetDocValues vals = DocValues.getSortedSet(reader, this.field);
      if (vals.advance(sortDoc.docId) != sortDoc.docId) return false;
      out.put(this.field,
          (IteratorWriter) w -> {
            long o;
            while((o = vals.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              BytesRef ref = vals.lookupOrd(o);
              fieldType.indexedToReadable(ref, cref);
              IndexableField f = fieldType.createField(schemaField, cref.toString());
              if (f == null) w.add(cref.toString());
              else w.add(fieldType.toObject(f));
            }
          });
      return true;
    }

  }


  static LongFunction<Object> bitsToValue(FieldType fieldType) {
    switch (fieldType.getNumberType()) {
      case LONG:
        return (bits)-> bits;
      case DATE:
        return (bits)-> new Date(bits);
      case INTEGER:
        return (bits)-> (int)bits;
      case FLOAT:
        return (bits)-> NumericUtils.sortableIntToFloat((int)bits);
      case DOUBLE:
        return (bits)-> NumericUtils.sortableLongToDouble(bits);
      default:
        throw new AssertionError("Unsupported NumberType: " + fieldType.getNumberType());
    }
  }
}
