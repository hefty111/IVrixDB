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
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.concurrent.*;

import org.apache.solr.client.solrj.io.*;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.*;
import org.apache.solr.handler.*;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.TimeBounds;
import org.apache.solr.ivrixdb.search.job.*;
import org.apache.solr.ivrixdb.search.utilities.StreamDefinitionsHolder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * This request handler is the main entry point for using IVrixDB's
 * {@link org.apache.solr.ivrixdb.search.job.SearchJob}. Search Jobs
 * represent a search that the user requested, and will hold the
 * resulting objects of the search. This handler is able to create
 * Search Jobs and poll the objects made by the search
 * (field summaries, timeline, events, results, etc.)
 *
 * @author Ivri Faitelson
 */
public class SearchJobRequestHandler extends RequestHandlerBase implements SolrCoreAware {
  private SolrDefaultStreamFactory streamFactory = new SolrDefaultStreamFactory();
  private static SolrClientCache clientCache = new SolrClientCache();
  private static ConcurrentMap objectCache = new ConcurrentHashMap();
  private static ModelCache modelCache = null;
  private String coreName;
  private enum JobAction {
    CREATE_JOB,
    DELETE_JOB,
    GET_TIMELINE,
    GET_RESULTS,
    GET_EVENTS,
    GET_FIELD_SUMMARIES
  }

  /**
   * Initializes the Search Job handler with Solr's Streaming API and IVrixDB's extension of it
   */
  public void inform(SolrCore core) {
    String defaultZkhost;
    String defaultCollection;
    CoreContainer coreContainer = core.getCoreContainer();

    if (coreContainer.isZooKeeperAware()) {
      defaultCollection = core.getCoreDescriptor().getCollectionName();
      defaultZkhost = core.getCoreContainer().getZkController().getZkServerAddress();
      this.streamFactory.withCollectionZkHost(defaultCollection, defaultZkhost);
      this.streamFactory.withDefaultZkHost(defaultZkhost);
      modelCache = new ModelCache(250,
          defaultZkhost,
          clientCache);
    }

    this.coreName = core.getName();
    this.streamFactory.withSolrResourceLoader(core.getResourceLoader());
    for (StreamDefinitionsHolder.StreamingFunction streamingFunction : StreamDefinitionsHolder.streamingFunctions.values()) {
      this.streamFactory.withFunctionName(streamingFunction.getName(), streamingFunction.getFunctionClass());
    }
  }

  /**
   * Closes the Search Job handler and all Streaming API related objects
   */
  public static void close() {
    clientCache.close();
  }

  /**
   * Performs the requested Search Job action, given request parameters.
   *
   * @param req Solr's request object. Contains many properties, including http parameters and residing core.
   * @param rsp Solr's response object. Will be used to add the response of the request.
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    JobAction jobAction = JobAction.valueOf(params.get("action"));

    switch (jobAction) {
      case CREATE_JOB:
        handleCreateJob(req, rsp);
        break;
      case GET_EVENTS:
        handleGetEvents(req, rsp);
        break;
      case GET_RESULTS:
        handleGetResults(req, rsp);
        break;
      case GET_TIMELINE:
        handleGetTimeline(req, rsp);
        break;
      case GET_FIELD_SUMMARIES:
        handleGetFieldSummaries(req, rsp);
        break;
    }
  }

  private void handleCreateJob(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    SolrParams params = req.getParams();
    StreamContext context = createStreamContext(req, params);
    StreamExpression streamExpression = StreamExpressionParser.parse(params.get("expr"));

    String earliestTimeString = params.get("earliest");
    String latestTimeString = params.get("latest");
    TimeBounds searchRange;

    if (earliestTimeString != null && latestTimeString != null) {
      searchRange = new TimeBounds(OffsetDateTime.parse(earliestTimeString), OffsetDateTime.parse(latestTimeString));
    } else {
      searchRange = new TimeBounds(false);
    }

    SearchJob newSearchJob = new SearchJob(streamExpression, searchRange, streamFactory, context);
    IVrixLocalNode.storeSearchJob(newSearchJob);
    newSearchJob.start();

    rsp.add("new-job-ID", newSearchJob.getJobID());
  }

  private void handleGetEvents(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    SolrParams params = req.getParams();
    String jobID = params.get("ID");
    int offset = params.getInt("offset");
    int size = params.getInt("size");
    String earliestTimeString = params.get("earliest");
    String latestTimeString = params.get("latest");
    SearchJob searchJob = IVrixLocalNode.getSearchJob(jobID);

    Collection<Tuple> events;
    if (earliestTimeString != null && latestTimeString != null) {
      events = searchJob.getStoredEvents(offset, size, OffsetDateTime.parse(earliestTimeString), OffsetDateTime.parse(latestTimeString));
    } else {
      events = searchJob.getStoredEvents(offset, size);
    }

    rsp.add("events", events);
  }

  private void handleGetResults(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    SolrParams params = req.getParams();
    String jobID = params.get("ID");
    int offset = params.getInt("offset");
    int size = params.getInt("size");

    SearchJob searchJob = IVrixLocalNode.getSearchJob(jobID);
    rsp.add("results", searchJob.getStoredResults(offset, size));
  }

  private void handleGetTimeline(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    SolrParams params = req.getParams();
    String jobID = params.get("ID");

    SearchJob searchJob = IVrixLocalNode.getSearchJob(jobID);
    rsp.add("timeline", searchJob.getTimelinePresentation());
  }

  private void handleGetFieldSummaries(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    SolrParams params = req.getParams();
    String jobID = params.get("ID");

    SearchJob searchJob = IVrixLocalNode.getSearchJob(jobID);
    rsp.add("field-summaries", searchJob.getFieldSummariesPresentation());
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
    context.put("core", coreName);
    context.put("solr-core", req.getCore());
    context.setLocal(local);
    return context;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return "handles all search-job related actions";
  }
}
