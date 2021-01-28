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

package org.apache.solr.ivrixdb.index.parse;

import java.io.IOException;
import java.util.Iterator;

import org.apache.solr.common.*;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;

/**
 * This object encapsulates event stream readers that read a
 * content-stream and spit out events in an iterative fashion.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: refactor -- remove SolrQueryRequest and just take in as a parameter the ContentStream...
 */
public abstract class EventStreamReader {
  /**
   * @param req Solr's request object. Needed for getting the content-stream.
   * @throws IOException If it couldn't open the content-stream and read it
   */
  public static EventStreamReader createReader(SolrQueryRequest req) throws IOException {
    EventStreamReader reader = null;

    Iterator<ContentStream> streams = req.getContentStreams().iterator();
    if (streams.hasNext()) {
      ContentStream stream = streams.next();
      reader = new JSONEventStreamReader(stream);
    }

    if (reader == null) {
      throw new IOException("no content stream was provided");
    }

    return reader;
  }

  /**
   * @return Whether there's a next event
   * @throws IOException When it couldn't retrieve from content-stream reader
   */
  public abstract boolean hasNext() throws IOException;

  /**
   * @return The next event
   * @throws IOException If it failed to parse the next document
   */
  public abstract SolrInputDocument next() throws IOException;

  /**
   * closes the reader
   */
  public abstract void close();
}
