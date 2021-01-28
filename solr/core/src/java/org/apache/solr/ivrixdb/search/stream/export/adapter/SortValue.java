
package org.apache.solr.ivrixdb.search.stream.export.adapter;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;

/**
 * enables operations on a sort value.
 * A sort value is a value extracted
 * from a field for sorting.
 */
public interface SortValue extends Comparable<SortValue> {
  public void setCurrentValue(int docId) throws IOException;
  public void setNextReader(LeafReaderContext context) throws IOException;
  public void setCurrentValue(SortValue value);
  public void reset();
  public SortValue copy();
  public Object getCurrentValue() throws IOException;
  public String getField();

  /**
   *
   * @return true if document has a value for the specified field
   */
  public boolean isPresent();
}