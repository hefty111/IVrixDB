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

package org.apache.solr.ivrixdb.index.node.update;

import java.io.IOException;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.ivrixdb.utilities.SolrObjectMocker;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.*;
import org.apache.solr.update.processor.*;

/**
 * This object updates buckets by wrapping around
 * Solr's update processor chain so that updating
 * commands do not need to be sent over HTTP.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: BUG FIX -- DistributedUpdatesAsyncException thrown when calling finish()
 *                  Due to this bug, this object is not being used at the moment
 *
 * TODO: Figure out where to add an optimization command for merging of Solr's index segments
 */
public class LocalBucketUpdater extends BucketUpdater {
  private final UpdateRequestProcessor processor;
  private final SolrQueryRequest mockRequest;

  /**
   * Creates a Solr update processor chain that operates on
   * a desired IVrix bucket by creating a mock SolrQueryRequest with
   * a new SolrCore that belongs to that IVrix bucket.
   *
   * @param core The name of the solr core under the collection to update.
   * @param rsp Solr's response object. Needed for creating the processor chain.
   */
  LocalBucketUpdater(SolrCore core, SolrQueryResponse rsp) {
    mockRequest = SolrObjectMocker.mockSolrUpdateRequest(core);
    UpdateRequestProcessorChain processorChain = mockRequest.getCore().getUpdateProcessorChain(mockRequest.getParams());
    processor = processorChain.createProcessor(mockRequest, rsp);
  }

  /**
   * {@inheritDoc}
   */
  public void addEvent(SolrInputDocument event) throws IOException {
    AddUpdateCommand updateCommand = new AddUpdateCommand(mockRequest);
    updateCommand.solrDoc = event;
    processor.processAdd(updateCommand);
  }

  /**
   * {@inheritDoc}
   */
  public void commitChanges() throws IOException {
    CommitUpdateCommand updateCommand = new CommitUpdateCommand(mockRequest, false);
    processor.processCommit(updateCommand);
  }

  /**
   *{@inheritDoc}
   */
  public void finish() throws IOException {
    processor.finish();
  }

  /**
   * {@inheritDoc}
   */
  public void close() throws IOException {
    processor.close();
  }
}
