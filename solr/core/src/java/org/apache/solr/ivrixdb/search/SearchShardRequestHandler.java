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

import java.util.concurrent.*;

import org.apache.solr.client.solrj.io.*;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.*;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.ivrixdb.search.stream.stfe.FieldExtractionStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * represents a possible alternative for IVrixExportRequestHandler.
 * re-sends export results back into the same shard for
 * Streaming API functions.
 *
 * @author Ivri Faitelson
 */
@Deprecated
public class SearchShardRequestHandler extends RequestHandlerBase implements SolrCoreAware {
  static SolrClientCache clientCache = new SolrClientCache();
  static ModelCache modelCache = null;
  static ConcurrentMap objectCache = new ConcurrentHashMap();
  private String coreName;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String urlWithoutHandler = getURLWithoutCurrentHandler(req);
    SolrParams params = req.getParams();

    TupleStream tupleStream = new SolrStream(urlWithoutHandler, params);
    tupleStream = new FieldExtractionStream(tupleStream);
    tupleStream = new ExceptionStream(tupleStream);
    tupleStream.setStreamContext(createStreamContext(req, params));

    rsp.add("result-set", tupleStream);
  }

  private String getURLWithoutCurrentHandler(SolrQueryRequest req) {
    String originalUrl = req.getHttpSolrCall().getReq().getRequestURL().toString();
    return originalUrl.substring(0, originalUrl.indexOf("/streamShard"));
  }

  private StreamContext createStreamContext(SolrQueryRequest req, SolrParams params) {
    int worker = params.getInt("workerID", 0);
    int numWorkers = params.getInt("numWorkers", 1);
    boolean local = params.getBool("streamLocalOnly", false);
    StreamContext context = new StreamContext();
    context.workerID = worker;
    context.numWorkers = numWorkers;
    context.setSolrClientCache(clientCache);
    context.setModelCache(modelCache);
    context.setObjectCache(objectCache);
    context.put("core", this.coreName);
    context.put("solr-core", req.getCore());
    context.setLocal(local);
    return context;
  }

  public void inform(SolrCore core) {
    String defaultZkhost;
    CoreContainer coreContainer = core.getCoreContainer();
    this.coreName = core.getName();

    if (coreContainer.isZooKeeperAware()) {
      defaultZkhost = core.getCoreContainer().getZkController().getZkServerAddress();
      modelCache = new ModelCache(250,
          defaultZkhost,
          clientCache);
    }

    core.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) { }
      @Override
      public void postClose(SolrCore core) {
        clientCache.close();
      }
    });
  }

  @Override
  public String getDescription() {
    return null;
  }
}
