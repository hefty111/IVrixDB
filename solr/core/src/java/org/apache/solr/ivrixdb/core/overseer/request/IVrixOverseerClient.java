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

package org.apache.solr.ivrixdb.core.overseer.request;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.ivrixdb.core.IVrixAdminRequestHandler;
import org.apache.solr.ivrixdb.core.IVrixAdminRequestHandler.AdminAction;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.election.LeaderElectionCandidate;
import org.apache.solr.ivrixdb.utilities.Constants;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.*;

/**
 * This client serves to remove the necessity of requesters
 * to know which IVrix Node is the IVrix Overseer. It can
 * handle remote and local requests.
 *
 * @author Ivri Faitelson
 */
public class IVrixOverseerClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static HttpSolrClient leaderClient = null;

  /**
   * Handles a request made over HTTP
   */
  public static void handleRemoteRequest(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    MultiMapSolrParams requestParams = (MultiMapSolrParams)req.getParams();
    String operationParamsString = requestParams.get(IVrixOverseerRemoteRequest.OPERATION_PARAMS_FIELD);
    IVrixOverseerOperationParams operationParams = IVrixOverseerOperationParams.fromJSONString(operationParamsString);

    Object response = tryExecuteWithWritingLogs(operationParams);
    rsp.add(IVrixOverseerRemoteResponse.RESPONSE_FIELD, response);
  }

  /**
   * Executes a request against the Overseer.
   *
   * If the Overseer is on the same node as the client,
   * then the client will execute the request directly.
   *
   * If the Overseer is remote, then the client will find which
   * Node is the Overseer and re-send the request there, where
   * that Node's Overseer client will handle that request.
   * If there was a connection loss at the beginning or during
   * the request, it will wait for a new IVrix Overseer to boot,
   * or continues after a timeout, and will try the request again.
   */
  public static Object execute(IVrixOverseerOperationParams params) throws Exception {
    try {
      return tryExecuteWithWritingLogsButNotConnectionLoss(params);
    } catch (LostConnectionToLeaderException e) {
      IVrixLocalNode.getOverseerElection().waitForReelection();
      return tryExecuteWithWritingLogs(params);
    }
  }




  private static Object tryExecuteWithWritingLogsButNotConnectionLoss(IVrixOverseerOperationParams params) throws Exception {
    return tryExecuteWithWritingLogs(params, false);
  }

  private static Object tryExecuteWithWritingLogs(IVrixOverseerOperationParams params) throws Exception {
    return tryExecuteWithWritingLogs(params, true);
  }

  private static Object tryExecuteWithWritingLogs(IVrixOverseerOperationParams params, boolean shouldLogConnectionLoss) throws Exception {
    Object response;
    try {
      log.info("executing operation " + params.getOperationType() + " -- " + params.toString() + " -- ...");
      response = tryExecute(params);
      log.info("operation " + params.getOperationType() + " -- " + params.toString() + " -- resulted in: " + response.toString());

    } catch (Exception e) {
      if (!(e instanceof LostConnectionToLeaderException && !shouldLogConnectionLoss)) {
        log.error("operation " + params.getOperationType() + " -- " + params.toString() + " -- failed with: ", e);
      }
      e.printStackTrace();
      throw e;
    }
    return response;
  }





  private static Object tryExecute(IVrixOverseerOperationParams params) throws Exception {
    LeaderElectionCandidate.OperatingMode localOverseerOperatingMode = IVrixLocalNode.getIVrixOverseer().getOperatingMode();
    if (localOverseerOperatingMode == LeaderElectionCandidate.OperatingMode.LEADER) {
      return executeAgainstLocalOverseer(params);
    } else if (localOverseerOperatingMode == LeaderElectionCandidate.OperatingMode.SLAVE) {
      return executeAgainstRemoteOverseer(params);
    } else {
      throw new IllegalAccessException("local node cannot execute overseer operations and is not allowed to route to leader");
    }
  }

  private static Object executeAgainstLocalOverseer(IVrixOverseerOperationParams params) throws Exception {
    IVrixOverseer overseer = IVrixLocalNode.getIVrixOverseer();
    return overseer.execute(params);
  }

  private static Object executeAgainstRemoteOverseer(IVrixOverseerOperationParams params) throws Exception {
    updateLeaderClientIfNecessary();
    IVrixOverseerRemoteRequest request = new IVrixOverseerRemoteRequest(params);
    return executeRemoteRequest(request);
  }





  private static void updateLeaderClientIfNecessary() throws IOException {
    String leaderBaseURL = IVrixLocalNode.getOverseerElection().getLeaderBaseURL();
    if (leaderClient == null || !leaderClient.getBaseURL().equals(leaderBaseURL)) {
      if (leaderClient != null) {
        leaderClient.close();
      }
      leaderClient = new HttpSolrClient.Builder(leaderBaseURL).build();
    }
  }

  private static Object executeRemoteRequest(IVrixOverseerRemoteRequest request) throws Exception {
    try {
      NamedList<Object> rawResponse = leaderClient.request(request, Constants.IVrix.CONTROLLER_COLLECTION_NAME);
      IVrixOverseerRemoteResponse response = new IVrixOverseerRemoteResponse();
      response.setResponse(rawResponse);
      return response.getOverseerResponse();

    } catch (SolrServerException e) {
      if (LostConnectionToLeaderException.isLostConnection(e)) {
        throw new LostConnectionToLeaderException(e);
      } else {
        throw e;
      }
    }
  }





  private static class LostConnectionToLeaderException extends Exception {
    public static final String DEFINING_MESSAGE_1 = "Server refused connection at";
    public static final String DEFINING_MESSAGE_2 = "Connection pool shut down";
    public LostConnectionToLeaderException(Exception e) {
      super(e);
    }
    public static boolean isLostConnection(Exception e) {
      return e.getMessage().contains(LostConnectionToLeaderException.DEFINING_MESSAGE_1) ||
             e.getMessage().contains(LostConnectionToLeaderException.DEFINING_MESSAGE_2);
    }
  }

  private static class IVrixOverseerRemoteResponse extends SolrResponseBase {
    public static final String RESPONSE_FIELD = "overseer-response";
    public Object getOverseerResponse() {
      return getResponse().get(RESPONSE_FIELD);
    }
  }

  private static class IVrixOverseerRemoteRequest extends SolrRequest<IVrixOverseerRemoteResponse> {
    public static final String OPERATION_PARAMS_FIELD = "operationParams";
    private final IVrixOverseerOperationParams operationParams;

    public IVrixOverseerRemoteRequest(IVrixOverseerOperationParams operationParams) {
      super(METHOD.GET, IVrixAdminRequestHandler.HANDLER_PATH);
      this.operationParams = operationParams;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(IVrixAdminRequestHandler.ACTION_FIELD, AdminAction.__INTERNAL_OVERSEER_COMMUNICATION.toString());
      params.add(OPERATION_PARAMS_FIELD, IVrixOverseerOperationParams.toJSONString(operationParams));
      return params;
    }

    @Override
    protected IVrixOverseerRemoteResponse createResponse(SolrClient client) {
      return new IVrixOverseerRemoteResponse();
    }
  }
}
