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

import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.ivrixdb.utilities.Constants;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * This handler is a workaround to boot up the IVrix Node.
 *
 * @author Ivri Faitelson
 */
public class IVrixLocalNodeInitializer extends RequestHandlerBase implements SolrCoreAware {
  /**
   * If the residing SolrCore of this handler belongs to the
   * IVrix Controller Collection, then the IVrix Node will boot up
   */
  @Override
  public void inform(SolrCore core) {
    String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    if (collectionName.equals(Constants.IVrix.CONTROLLER_COLLECTION_NAME)) {
      IVrixLocalNode.init(core.getCoreContainer(), core);
    }
  }

  /**
   * Does nothing
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {}

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return "Workaround to to boot up the IVrix Node";
  }
}
