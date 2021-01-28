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

import java.io.*;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.*;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.JsonLoader;
import org.noggit.JSONParser;

/**
 * This object reads a JSON content-stream and spits out events in an iterative fashion.
 *
 * @author Ivri Faitelson
 */
public class JSONEventStreamReader extends EventStreamReader {
  private final JSONParser parser;
  private final Reader streamReader;
  private int currentJSONReaderEvent;

  /**
   * @param stream The content stream to read.
   * @throws IOException If it couldn't open the content-stream and read it.
   */
  public JSONEventStreamReader(ContentStream stream) throws IOException {
    streamReader = stream.getReader();
    parser = new JSONParser(streamReader);

    int parserEvent = parser.nextEvent();
    assert parserEvent == JSONParser.ARRAY_START;
  }

  /**
   * {@inheritDoc}
   */
  public boolean hasNext() throws IOException {
    currentJSONReaderEvent = parser.nextEvent();
    return currentJSONReaderEvent != JSONParser.ARRAY_END && currentJSONReaderEvent != JSONParser.EOF;
  }

  /**
   * {@inheritDoc}
   */
  public SolrInputDocument next() throws IOException {
    return parseDoc(currentJSONReaderEvent);
  }

  /**
   * {@inheritDoc}
   */
  public void close() {
    IOUtils.closeQuietly(streamReader);
  }



  // a copy of JsonLoader.parseDoc()
  private SolrInputDocument parseDoc(int ev) throws IOException {
    assert ev == JSONParser.OBJECT_START;
    SolrInputDocument sdoc = new SolrInputDocument();
    for (; ; ) {
      ev = parser.nextEvent();
      if (ev == JSONParser.OBJECT_END) {
        return sdoc;
      }
      String fieldName = parser.getString();
      if (fieldName.equals(JsonLoader.CHILD_DOC_KEY)) {
        ev = parser.nextEvent();
        while ((ev = parser.nextEvent()) != JSONParser.ARRAY_END) {
          sdoc.addChildDocument(parseDoc(ev));
        }
      } else {
        ev = parser.nextEvent();
        Object val = parseFieldValue(ev, fieldName);
        sdoc.addField(fieldName, val);
      }
    }
  }

  // a copy of JsonLoader.getString()
  private Object parseFieldValue(int ev, String fieldName) throws IOException {
    switch (ev) {
      case JSONParser.STRING:
        return parser.getString();
      case JSONParser.LONG:
        return parser.getLong();
      case JSONParser.NUMBER:
        return parser.getDouble();
      case JSONParser.BIGNUMBER:
        return parser.getNumberChars().toString();
      case JSONParser.BOOLEAN:
        return parser.getBoolean();
      case JSONParser.NULL:
        parser.getNull();
        return null;
      default:
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing JSON field value. "
            + "Unexpected " + JSONParser.getEventString(ev) + " at [" + parser.getPosition() + "], field=" + fieldName);
    }
  }
}
