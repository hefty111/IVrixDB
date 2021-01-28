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

package org.apache.solr.ivrixdb.search;

import java.io.IOException;

import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.*;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.ivrixdb.search.stream.export.ExportAdapterStream;
import org.apache.solr.ivrixdb.search.stream.stfe.FieldExtractionStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * This request handler combines Solr's export handler and the Streaming API
 * in order to do shard-level operations before the results are streamed over
 * http.
 *
 * @author Ivri Faitelson
 *
 * TODO -- IMPORTANT NOTE: This handler was ONLY experimental. the new decorator called "drill" may replace this code.
 */
public class IVrixExportRequestHandler extends RequestHandlerBase implements SolrCoreAware {
  private final SearchHandler searchHandler = new SearchHandler();

  /**
   * Creates a streaming pipeline that passes Tuples down without using http.
   * Searches the index, uses decorators to manipulate tuples, and returns them.
   *
   * @param req Solr's request object. Contains many properties, including query parameters and residing core.
   * @param rsp Solr's response object. Will be used to add the streaming response.
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    searchIndex(req, rsp);
    
    TupleStream tupleStream = createExportAdapterStream(req);
    tupleStream = new FieldExtractionStream(tupleStream);
    tupleStream = new ExceptionStream(tupleStream);
    rsp.add("result-set", tupleStream);
  }

  /**
   * Uses Solr's SearchHandler to search the index and add
   * the result of the query to Solr's request object.
   */
  private void searchIndex(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
    params.add("distrib", "false");
    params.add("rq", "{!xport}");
    req.setParams(params);

    try {
      searchHandler.handleRequestBody(req, rsp);
    } catch (Exception e) {
      rsp.setException(e);
    }
    Exception exception = rsp.getException();
    if (exception != null) {
      throw new IOException(exception);
    }

    NamedList responses = rsp.getValues();
    responses.remove(1);
    rsp.setAllValues(responses);
  }

  private ExportAdapterStream createExportAdapterStream(SolrQueryRequest req) throws IOException {
    SolrIndexSearcher searcher = req.getSearcher();
    FixedBitSet[] exportBitSets = null;
    int totalHits = 0;

    if (req.getContext().get("totalHits") != null) {
      totalHits = (Integer) req.getContext().get("totalHits");
      exportBitSets = (FixedBitSet[]) req.getContext().get("export");
      if (exportBitSets == null) {
        throw new IOException("xport RankQuery is required for xsort: rq={!xport}");
      }
    }

    return new ExportAdapterStream(searcher, exportBitSets, totalHits);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return "Provides the Streaming API at the equal level of an export handler";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void inform(SolrCore core) {
    searchHandler.inform(core);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(NamedList args) {
    super.init(args);
    searchHandler.init(args);
  }
}
