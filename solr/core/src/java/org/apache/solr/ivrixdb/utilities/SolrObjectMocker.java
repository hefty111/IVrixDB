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

package org.apache.solr.ivrixdb.utilities;

import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.*;

/**
 * This class is a collection of factory functions that mock internal objects
 * in Solr so that IVrixDB can reuse crucial and effective Solr code.
 * These mocks are intended and designed to be harmless and replicate Solr behavior.
 *
 * @author Ivri Faitelson
 */
public class SolrObjectMocker {
  /**
   * @return A Solr Streaming API EOF tuple
   */
  public static Tuple mockEOFTuple() {
    Map eof = new HashMap();
    eof.put(Constants.Solr.EOF_STR, true);
    return new Tuple(eof);
  }

  /**
   * @return An object that mocks an Update request to a Solr Collection
   */
  public static SolrQueryRequest mockSolrUpdateRequest(SolrCore core) {
    HashMap<String, String> updateParams = new HashMap<>();
    updateParams.put("commit", "true");
    return new SolrQueryRequestBase(core, new MapSolrParams(updateParams)) {};
  }
}
