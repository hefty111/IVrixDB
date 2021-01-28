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

package org.apache.solr.ivrixdb.search.stream.export;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.search.*;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.ivrixdb.search.stream.export.adapter.ExportAdapter;
import org.apache.solr.ivrixdb.utilities.Constants;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * This stream utilizes a modified {@link org.apache.solr.handler.export.ExportWriter}
 * that efficiently extracts one Tuple at a time for the purposes of IVrixDB.
 *
 * @author Ivri Faitelson
 */
public class ExportAdapterStream extends TupleStream {
  private static final String[] exportFields = new String[]{Constants.IVrix.RAW_TIME_FIELD, Constants.IVrix.RAW_EVENT_FIELD};
  private static final Sort descSort = new Sort(new SortField(Constants.IVrix.RAW_TIME_FIELD, SortField.Type.LONG, true));
  private final ExportAdapter exportAdapter;

  /**
   * creates an export adapter with settings (fl="_time,_raw", sort="_time desc")
   */
  public ExportAdapterStream(SolrIndexSearcher indexSearcher, FixedBitSet[] exportBitSets, int totalHits) throws IOException {
    Sort sortOrder = indexSearcher.weightSort(descSort);
    exportAdapter = new ExportAdapter(exportFields, sortOrder, indexSearcher, exportBitSets, totalHits);
  }

  /**
   * Reads the next tuple from the export adapter
   */
  public Tuple read() throws IOException {
    return exportAdapter.nextTuple();
  }



  /**
   * Does nothing
   */
  @Override
  public void open() throws IOException {}

  /**
   * Does nothing
   */
  @Override
  public void close() throws IOException {}

  /**
   * Does nothing
   */
  @Override
  public void setStreamContext(StreamContext context) {}

  /**
   * {@inheritDoc}
   */
  @Override
  public List<TupleStream> children() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return null;
  }
}
