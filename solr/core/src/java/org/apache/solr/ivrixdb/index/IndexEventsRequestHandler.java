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

package org.apache.solr.ivrixdb.index;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket;
import org.apache.solr.ivrixdb.index.forward.EventForwarder;
import org.apache.solr.ivrixdb.index.node.EventIndexer;
import org.apache.solr.ivrixdb.index.parse.EventStreamReader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * This request handler is the main entry-point for IVrixDB indexing.
 * It takes in a stream of events from the http request, reads them in an
 * iterative fashion, and indexes them into buckets. Once certain criteria meet,
 * like maximum events in a bucket, it will finish indexing into a bucket,
 * create a new one, and continue to index the new one.
 *
 * All IVrix Nodes manage their own buckets, meaning that this handler
 * will only index into the buckets belonging to its residing Node.
 *
 * It also allows the user to load-balance events across IVrix Nodes.
 * This is done by batching events and sending them out in a round-robin.
 *
 * To learn more about IVrix's buckets, please refer to {@link IVrixBucket}.
 *
 * @author Ivri Faitelson
 */
public class IndexEventsRequestHandler extends RequestHandlerBase {
  public static final String HANDLER_PATH = "/indexEvents";
  public static final String INDEX_NAME_FIELD = "indexName";
  public static final String LOAD_BALANCE_FIELD = "toLoadBalance";

  /**
   * Takes the content-streams from http request, reads them in an iterative fashion to
   * produce one event at a time. Depending on a parameter, it will either index directly into
   * local HOT buckets or batch the events and send them out to other IVrix Nodes in a round-robin.
   *
   * @param req Solr's request object. Contains many properties, including content-streams, and request parameters.
   * @param rsp Solr's response object. Will be used to add the response of the request.
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams requestParams = req.getParams();
    String indexName = requestParams.get(INDEX_NAME_FIELD);
    Boolean toLoadBalance = requestParams.getBool(LOAD_BALANCE_FIELD);

    EventStreamReader streamReader = EventStreamReader.createReader(req);

    if (toLoadBalance != null && toLoadBalance) {
      loadBalanceStream(streamReader, indexName);
    } else {
      indexStream(streamReader, indexName);
    }
  }

  private void loadBalanceStream(EventStreamReader streamReader, String indexName) throws Exception {
    EventForwarder forwarder = new EventForwarder(indexName);
    try {
      while (streamReader.hasNext()) {
        SolrInputDocument event = streamReader.next();
        forwarder.forwardEvent(event);
      }
    } finally {
      streamReader.close();
      forwarder.close();
    }
  }

  private void indexStream(EventStreamReader streamReader, String indexName) throws Exception {
    EventIndexer indexer = new EventIndexer(indexName);
    try {
      indexer.prepareForBatch();
      while (streamReader.hasNext()) {
        SolrInputDocument event = streamReader.next();
        indexer.index(event);
      }
    } finally {
      streamReader.close();
      indexer.close();
    }
  }



  /**
   * uses Solr's code to initialize the handler.
   * Turns off HTTP caching (example taken from {@link org.apache.solr.handler.ContentStreamHandlerBase}.)
   */
  @Override
  public void init(NamedList args) {
    super.init(args);
    httpCaching = false;
    if (args != null) {
      Object caching = args.get("httpCaching");
      if(caching!=null) {
        httpCaching = Boolean.parseBoolean(caching.toString());
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return "Index time-series events to collections dynamically";
  }
}
