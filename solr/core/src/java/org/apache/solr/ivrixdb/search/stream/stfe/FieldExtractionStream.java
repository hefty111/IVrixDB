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

package org.apache.solr.ivrixdb.search.stream.stfe;

import java.io.IOException;
import java.math.*;
import java.util.*;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.ivrixdb.utilities.Constants;

/**
 * This stream reads raw events from an inner stream
 * and extracts new fields using configurable regexes.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: make the extractions with configurable regexes
 */
public class FieldExtractionStream extends TupleStream implements Expressible {
  private final TupleStream innerStream;

  /**
   * @param innerStream The inner stream that must return event tuples
   */
  public FieldExtractionStream(TupleStream innerStream) {
    this.innerStream = innerStream;
  }

  /**
   * Reads a Tuple from the inner stream and extracts new fields
   * from IVrixDB's raw event field using configurable regexps.
   * The values of the fields are either long, double, or String
   * (as to not interfere with Solr's decorators).
   **/
  public Tuple read() throws IOException {
    Tuple newTuple;

    Tuple tuple = innerStream.read();
    if (tuple.EOF) {
      newTuple = tuple;

    } else {
      Map rawFields = tuple.getMap();
      Map extractedFields = createMapWithFieldExtraction(rawFields);
      newTuple = new Tuple(extractedFields);
    }

    return newTuple;
  }

  private Map createMapWithFieldExtraction(Map fields) throws IOException {
    String text = (String)fields.get(Constants.IVrix.RAW_EVENT_FIELD);
    String[] fieldStringList = text.split("\\s+");

    for (String fieldString : fieldStringList) {
      String[] field = fieldString.split(":");

      String key = field[0];
      Object value = typeCheck(field[1]);

      fields.put(key, value);
    }

    return fields;
  }

  private Object typeCheck(String valueString) throws IOException {
    Object value;
    if (NumberUtils.isCreatable(valueString)) {
      Number number = NumberUtils.createNumber(valueString);

      boolean isIntType = number instanceof Long || number instanceof Integer || number instanceof BigInteger;
      boolean isDecimalType = number instanceof Double || number instanceof Float || number instanceof BigDecimal;

      if (isIntType) {
        value = number.longValue();
      } else if (isDecimalType) {
        value = number.doubleValue();
      } else {
        throw new IOException("Could not properly create number: " + valueString);
      }
    } else {
      value = valueString;
    }
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void open() throws IOException {
    innerStream.open();
  }

  /**
   * {@inheritDoc}
   */
  public void close() throws IOException {
    innerStream.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setStreamContext(StreamContext context) {
    this.innerStream.setStreamContext(context);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return innerStream.toExplanation(factory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return ((Expressible)innerStream).toExpression(factory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<TupleStream> children() {
    return innerStream.children();
  }


  /**
   * {@inheritDoc}
   */
  public StreamComparator getStreamSort(){
    return innerStream.getStreamSort();
  }
}
