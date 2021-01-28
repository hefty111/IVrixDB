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

package org.apache.solr.ivrixdb.core;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.ivrixdb.core.overseer.request.*;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;


/**
 * @author Ivri Faitelson
 */
public class IVrixAdminRequestHandler extends RequestHandlerBase {
  public static final String ACTION_FIELD = "adminAction";
  public static final String HANDLER_PATH = "/admin";

  /**
   * Defines an action that the admin handler can perform.
   * Can also be internal overseer communication.
   */
  public enum AdminAction {
    __INTERNAL_OVERSEER_COMMUNICATION,
    CREATE_INDEX,
    DELETE_INDEX,
    LIST_INDEXES
  }

  /**
   * Performs the requested Admin action, given request parameters.
   *
   * @param req Solr's request object. Contains many properties, including http parameters and residing core.
   * @param rsp Solr's response object. Will be used to add the response of the request.
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams requestParams = req.getParams();
    AdminAction adminAction = AdminAction.valueOf(requestParams.get(ACTION_FIELD));
    switch (adminAction) {
      case __INTERNAL_OVERSEER_COMMUNICATION:
        handleInternalOverseerCommunication(req, rsp);
        break;
      case CREATE_INDEX:
        handleCreationOfIndex(req, rsp);
        break;
      case DELETE_INDEX:
        handleDeletionOfIndex(req, rsp);
        break;
      case LIST_INDEXES:
        handleListAllIndexes(req, rsp);
        break;
    }
  }

  private void handleInternalOverseerCommunication(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    IVrixOverseerClient.handleRemoteRequest(req, rsp);
  }

  private void handleCreationOfIndex(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams requestParams = req.getParams();
    String indexName = requestParams.get("indexName");
    IVrixOverseerOperationParams createOperation = IVrixOverseerOperationParams.CreateIVrixIndex(indexName);
    rsp.add("response", IVrixOverseerClient.execute(createOperation));
  }

  private void handleDeletionOfIndex(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams requestParams = req.getParams();
    String indexName = requestParams.get("indexName");
    IVrixOverseerOperationParams deleteOperation = IVrixOverseerOperationParams.DeleteIVrixIndex(indexName);
    rsp.add("response", IVrixOverseerClient.execute(deleteOperation));
  }


  private void handleListAllIndexes(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    IVrixOverseerOperationParams listAllOperation = IVrixOverseerOperationParams.GetAllIVrixIndexes();
    rsp.add("response", IVrixOverseerClient.execute(listAllOperation));
  }



  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return "A handler for administration request to IVrix, and for internal Overseer communication";
  }
}
