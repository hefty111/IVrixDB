
package org.apache.solr.ivrixdb.search.stream.export.adapter;

import java.io.IOException;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.SortField;
import org.apache.solr.ivrixdb.search.stream.export.adapter.comp.*;
import org.apache.solr.ivrixdb.search.stream.export.adapter.doc.*;
import org.apache.solr.ivrixdb.search.stream.export.adapter.value.*;
import org.apache.solr.schema.*;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Stores the doc ID and sort values, and enables operations on them.
 */
public class SortDoc {
  public int docId = -1;
  public int ord = -1;
  public int docBase = -1;

  private SortValue[] sortValues;

  public SortDoc(SortValue[] sortValues) {
    this.sortValues = sortValues;
  }

  public SortDoc() {
  }


  public static SortDoc getSortDoc(SolrIndexSearcher searcher, SortField[] sortFields) throws IOException {
    SortValue[] sortValues = new SortValue[sortFields.length];
    IndexSchema schema = searcher.getSchema();
    for (int i = 0; i < sortFields.length; ++i) {
      SortField sf = sortFields[i];
      String field = sf.getField();
      boolean reverse = sf.getReverse();
      SchemaField schemaField = schema.getField(field);
      FieldType ft = schemaField.getType();

      if (!schemaField.hasDocValues()) {
        throw new IOException(field + " must have DocValues to use this feature.");
      }

      if (ft instanceof SortableTextField && schemaField.useDocValuesAsStored() == false) {
        throw new IOException(schemaField + " Must have useDocValuesAsStored='true' to be used with export writer");
      }

      if (ft instanceof IntValueFieldType) {
        if (reverse) {
          sortValues[i] = new IntValue(field, IntComp.desc());
        } else {
          sortValues[i] = new IntValue(field, IntComp.asc());
        }
      } else if (ft instanceof FloatValueFieldType) {
        if (reverse) {
          sortValues[i] = new FloatValue(field, FloatComp.desc());
        } else {
          sortValues[i] = new FloatValue(field, FloatComp.asc());
        }
      } else if (ft instanceof DoubleValueFieldType) {
        if (reverse) {
          sortValues[i] = new DoubleValue(field, DoubleComp.desc());
        } else {
          sortValues[i] = new DoubleValue(field, DoubleComp.asc());
        }
      } else if (ft instanceof LongValueFieldType) {
        if (reverse) {
          sortValues[i] = new LongValue(field, LongComp.desc());
        } else {
          sortValues[i] = new LongValue(field, LongComp.asc());
        }
      } else if (ft instanceof StrField || ft instanceof SortableTextField) {
        LeafReader reader = searcher.getSlowAtomicReader();
        SortedDocValues vals = reader.getSortedDocValues(field);
        if (reverse) {
          sortValues[i] = new StringValue(vals, field, IntComp.desc());
        } else {
          sortValues[i] = new StringValue(vals, field, IntComp.asc());
        }
      } else if (ft instanceof DateValueFieldType) {
        if (reverse) {
          sortValues[i] = new LongValue(field, LongComp.desc());
        } else {
          sortValues[i] = new LongValue(field, LongComp.asc());
        }
      } else if (ft instanceof BoolField) {
        // This is a bit of a hack, but since the boolean field stores ByteRefs, just like Strings
        // _and_ since "F" happens to sort before "T" (thus false sorts "less" than true)
        // we can just use the existing StringValue here.
        LeafReader reader = searcher.getSlowAtomicReader();
        SortedDocValues vals = reader.getSortedDocValues(field);
        if (reverse) {
          sortValues[i] = new StringValue(vals, field, IntComp.desc());
        } else {
          sortValues[i] = new StringValue(vals, field, IntComp.asc());
        }
      } else {
        throw new IOException("Sort fields must be one of the following types: int,float,long,double,string,date,boolean,SortableText");
      }
    }
    //SingleValueSortDoc etc are specialized classes which don't have array lookups. On benchmarking large datasets
    //This is faster than the using an array in SortDoc . So upto 4 sort fields we still want to keep specialized classes.
    //SOLR-12616 has more details
    if (sortValues.length == 1) {
      return new SingleValueSortDoc(sortValues[0]);
    } else if (sortValues.length == 2) {
      return new DoubleValueSortDoc(sortValues[0], sortValues[1]);
    } else if (sortValues.length == 3) {
      return new TripleValueSortDoc(sortValues[0], sortValues[1], sortValues[2]);
    } else if (sortValues.length == 4) {
      return new QuadValueSortDoc(sortValues[0], sortValues[1], sortValues[2], sortValues[3]);
    }
    return new SortDoc(sortValues);
  }

  public SortValue getSortValue(String field) {
    for (SortValue value : sortValues) {
      if (value.getField().equals(field)) {
        return value;
      }
    }
    return null;
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.ord = context.ord;
    this.docBase = context.docBase;
    for (SortValue value : sortValues) {
      value.setNextReader(context);
    }
  }

  public void reset() {
    this.docId = -1;
    this.docBase = -1;
    for (SortValue value : sortValues) {
      value.reset();
    }
  }

  public void setValues(int docId) throws IOException {
    this.docId = docId;
    for(SortValue sortValue : sortValues) {
      sortValue.setCurrentValue(docId);
    }
  }

  public void setValues(SortDoc sortDoc) {
    this.docId = sortDoc.docId;
    this.ord = sortDoc.ord;
    this.docBase = sortDoc.docBase;
    SortValue[] vals = sortDoc.sortValues;
    for(int i=0; i<vals.length; i++) {
      sortValues[i].setCurrentValue(vals[i]);
    }
  }

  public SortDoc copy() {
    SortValue[] svs = new SortValue[sortValues.length];
    for(int i=0; i<sortValues.length; i++) {
      svs[i] = sortValues[i].copy();
    }

    return new SortDoc(svs);
  }

  public boolean lessThan(Object o) {
    if(docId == -1) {
      return true;
    }
    SortDoc sd = (SortDoc)o;
    SortValue[] sortValues1 = sd.sortValues;
    for(int i=0; i<sortValues.length; i++) {
      int comp = sortValues[i].compareTo(sortValues1[i]);
      if (comp < 0) {
        return true;
      } else if (comp > 0) {
        return false;
      }
    }
    return docId+docBase > sd.docId+sd.docBase; //index order
  }

  public int compareTo(Object o) {
    SortDoc sd = (SortDoc)o;
    for (int i=0; i<sortValues.length; i++) {
      int comp = sortValues[i].compareTo(sd.sortValues[i]);
      if (comp != 0) {
        return comp;
      }
    }
    return 0;
  }


  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("docId: ").append(docId).append("; ");
    for (int i=0; i < sortValues.length; i++) {
      builder.append("value").append(i).append(": ").append(sortValues[i]).append(", ");
    }
    return builder.toString();
  }
}