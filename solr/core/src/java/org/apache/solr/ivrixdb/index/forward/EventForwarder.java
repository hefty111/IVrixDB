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

package org.apache.solr.ivrixdb.index.forward;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.*;


import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.Utils;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.index.IndexEventsRequestHandler;
import org.apache.solr.ivrixdb.utilities.Constants;
import org.slf4j.*;

/**
 * This object creates event batches from an event stream
 * and forwards (load-balances) them to IVrix Nodes in
 * a round-robin fashion. It is fault-tolerant, meaning
 * it can handle the failure of IVrix Nodes. When an
 * IVrix Node fails, it will take the batch that
 * was sent to it and re-send it to a live node.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: To Improve -- figure out best timeout length for a node client.
 *                     the reason why it was elongated in the first place
 *                     is because when a Node has died, the other node
 *                     clients that were utilizing that dead node would hang
 *                     for about 3 minutes. To be 100% positive that the timeout
 *                     was the problem, I set the timeout to 10 minutes, it the errors disappeared.
 *                     either reduce the timeout number, or fix the hanging in the first place...
 */
public class EventForwarder {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String indexName;
  private final List<Map<String, Object>> eventsBatch = new LinkedList<>();
  private final List<HttpSolrClient> clients = new LinkedList<>();
  private int indexOfCurrentClientName;

  /**
   * Creates a new forwarder with IVrix Node clients
   * @param indexName The name of the index to index into
   */
  public EventForwarder(String indexName) {
    this.indexName = indexName;
    this.indexOfCurrentClientName = 0;

    for (String liveNode : getURLsOfLiveNodes()) {
      final int timeout = 600000;
      this.clients.add(
          new HttpSolrClient.Builder(liveNode)
          .withConnectionTimeout(timeout)
          .withSocketTimeout(timeout)
          .build()
      );
    }
  }

  /**
   * Stores the event into a batch, and when the batch is large enough,
   * it will forward the batch to an IVrix Node for indexing.
   */
  public void forwardEvent(SolrInputDocument event) {
    Map<String, Object> eventMap = copyEventAsMap(event);
    eventsBatch.add(eventMap);
    if (eventsBatch.size() >= Constants.IVrix.DEFAULT_EVENT_BATCH_SIZE) {
      flushBatch();
    }
  }

  /**
   * Closes the forwarder. Forwards any non-forwarded batches,
   * and closes all the clients to the IVrix Nodes.
   */
  public void close() throws IOException {
    if (eventsBatch.size() > 0) {
      flushBatch();
    }
    for (HttpSolrClient client : clients) {
      client.close();
    }
  }





  private void flushBatch() {
    boolean batchSucceededToIndex = false;
    while (!batchSucceededToIndex) {
      batchSucceededToIndex = tryToIndexBatch();
    }
    eventsBatch.clear();
    prepareForNextClient();
  }

  private boolean tryToIndexBatch() {
    if (clients.size() == 0) {
      throw new IllegalStateException("The list of nodes to forward to should never be empty...");
    }

    boolean success;
    try {
      IndexEventsRequest request = new IndexEventsRequest(eventsBatch, indexName);
      clients.get(indexOfCurrentClientName).request(request, Constants.IVrix.CONTROLLER_COLLECTION_NAME);
      success = true;

    } catch (Exception e) {
      log.warn("Batch sent to a node failed to index. Removing that node from list of nodes to forward to. ", e);
      clients.remove(indexOfCurrentClientName);
      correctIndexOfCurrentClientIfNecessary();
      success = false;
    }

    return success;
  }

  private void prepareForNextClient() {
    indexOfCurrentClientName += 1;
    correctIndexOfCurrentClientIfNecessary();
  }

  private void correctIndexOfCurrentClientIfNecessary() {
    if (indexOfCurrentClientName > clients.size() - 1) {
      indexOfCurrentClientName = 0;
    }
  }






  private Map<String, Object> copyEventAsMap(SolrInputDocument event) {
    Map<String, Object> eventMap = new HashMap<>();
    eventMap.put(Constants.IVrix.RAW_TIME_FIELD, event.getFieldValue(Constants.IVrix.RAW_TIME_FIELD));
    eventMap.put(Constants.IVrix.RAW_EVENT_FIELD, event.getFieldValue(Constants.IVrix.RAW_EVENT_FIELD));
    return eventMap;
  }

  private List<String> getURLsOfLiveNodes() {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollection(Constants.IVrix.CONTROLLER_COLLECTION_NAME);
    List<String> baseURLsOfResidingNodes = new LinkedList<>();

    for (Slice shard : collectionState.getSlices()) {
      for (Replica replica : shard.getReplicas()) {
        String baseURL = replica.getBaseUrl();
        if (!baseURLsOfResidingNodes.contains(baseURL)) {
          baseURLsOfResidingNodes.add(replica.getBaseUrl());
        }
      }
    }
    return baseURLsOfResidingNodes;
  }







  private static class IndexEventsRequest extends SolrRequest<IndexEventsResponse> {
    private final List<Map<String, Object>> eventsBatch;
    private final String indexName;

    public IndexEventsRequest(List<Map<String, Object>> eventsBatch, String indexName) {
      super(METHOD.POST, IndexEventsRequestHandler.HANDLER_PATH);
      this.eventsBatch = eventsBatch;
      this.indexName = indexName;
    }

    public RequestWriter.ContentWriter getContentWriter(String expectedType) {
      return new RequestWriter.ContentWriter() {
        @Override
        public void write(OutputStream os) throws IOException {
          Utils.writeJson(eventsBatch, os, true);
        }
        @Override
        public String getContentType() {
          return ClientUtils.TEXT_JSON;
        }
      };
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(IndexEventsRequestHandler.INDEX_NAME_FIELD, indexName);
      return params;
    }

    @Override
    protected IndexEventsResponse createResponse(SolrClient client) {
      return new IndexEventsResponse();
    }
  }
  private static class IndexEventsResponse extends SolrResponseBase {}
}
