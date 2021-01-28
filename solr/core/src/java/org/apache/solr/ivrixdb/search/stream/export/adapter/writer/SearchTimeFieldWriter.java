/*
 * IVrixDB software is licensed under the IVrixDB Software License Agreement
 * (the "License"); you may not use this file or the IVrixDB except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     https://github.com/hefty111/IVrixDB/blob/master/LICENSE.pdf
 *
 * Unless required by applicable law or agreed to in writing, IVrixDB software is provided
 * and distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions
 * and limitations under the License. See the NOTICE file distributed with the IVrixDB Software
 * for additional information regarding copyright ownership.
 */

package org.apache.solr.ivrixdb.search.stream.export.adapter.writer;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.apache.solr.common.util.*;
import org.apache.solr.ivrixdb.search.stream.export.adapter.*;
import org.apache.solr.schema.FieldType;

/**
 * @author Ivri Faitelson
 */
@Deprecated
public class SearchTimeFieldWriter extends FieldWriter {
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

  public SearchTimeFieldWriter(String field, FieldType fieldType) {
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
      writeFieldExtraction(v, ew);
    }
    return true;
  }


  private void writeFieldExtraction(String raw, Map ew) {
    String[] fieldStringList = raw.split("\\s+");

    for (String fieldString : fieldStringList) {
      String[] field = fieldString.split(":");

      String key = field[0];
      //Object value = TupleValue.convert(field[1]);
      Object value = field[1];

      ew.put(key, value);
    }
  }


  private static class TupleValue {
    public static Object convert(String valueString) {
      Object value;
      if (isLong(valueString)) {
        value = Long.parseLong(valueString);
      } else if (isDouble(valueString)) {
        value = Double.parseDouble(valueString);
      } else {
        value = valueString;
      }
      return value;
    }

    private static boolean isLong(String value) {
      boolean valid;
      try {
        Long.parseLong(value);
        valid = true;
      } catch (NumberFormatException e) {
        valid = false;
      }
      return valid;
    }

    private static boolean isDouble(String value) {
      boolean valid;
      try {
        Double.parseDouble(value);
        valid = true;
      } catch (NumberFormatException e) {
        valid = false;
      }
      return valid;
    }
  }
}
