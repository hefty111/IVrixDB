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

package org.apache.solr.ivrixdb.search.stream;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.ivrixdb.search.stream.search.index.IVrixIndexSearchStream;
import org.apache.solr.ivrixdb.search.stream.stfe.FieldExtractionStream;

/**
 * This stream replaces {@link org.apache.solr.client.solrj.io.stream.SearchFacadeStream}
 * and allows searches on an IVrix index by combining a search layer and a field extraction layer.
 * It also enables a gateway for the normal search on a single collection.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: maybe add filtering layers so that when user requests (q:"extracted_field:'value'), it will ensure that only that value appears
 *       this depends on how the language will support filtering of search-time fields.
 */
public class IVrixSearchStream extends CloudSolrStream implements Expressible {
  private static final String IVRIX_SEARCH_PARAM = "ivrix";

  private TupleStream ivrixSearchPipeline;

  /**
   * creates from stream expression.
   */
  public IVrixSearchStream(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    init();
  }

  /**
   * creates from explicit config.
   */
  public IVrixSearchStream(String zkHost, String indexName, SolrParams params) throws IOException {
    super(zkHost, indexName, params);
    init();
  }

  private void init() throws IOException {
    if (params.getBool(IVRIX_SEARCH_PARAM, false)) {
      String indexName = collection;
      ivrixSearchPipeline = new IVrixIndexSearchStream(indexName, zkHost, params);
      ivrixSearchPipeline = new FieldExtractionStream(ivrixSearchPipeline);

    } else {
      ivrixSearchPipeline = new CloudSolrStream(zkHost, collection, params);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void open() throws IOException {
    ivrixSearchPipeline.open();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple read() throws IOException {
    return ivrixSearchPipeline.read();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    ivrixSearchPipeline.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<TupleStream> children() {
    return ivrixSearchPipeline.children();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setStreamContext(StreamContext context) {
    ivrixSearchPipeline.setStreamContext(context);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return ivrixSearchPipeline.toExplanation(factory);
  }
}
