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

package org.apache.solr.ivrixdb.search.stream;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.comp.*;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.ivrixdb.search.stream.nonstream.*;
import org.apache.solr.ivrixdb.search.stream.nonstream.processor.*;

import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * This stream replaces {@link org.apache.solr.client.solrj.io.stream.RankStream}
 * by re-implementing top-N sort with previewing capability.
 *
 * @author Ivri Faitelson
 */
public class IVrixSortStream extends NonStreamProcessorStream {
  private final SortSyntax sortSyntax = new SortSyntax(this);
  private StreamComparator comparator;
  private int limit;

  /**
   * creates from stream expression
   */
  public IVrixSortStream(StreamExpression expression, StreamFactory factory) throws IOException {
    SortSyntax.Config config = sortSyntax.extractConfig(expression, factory);
    init(config.tupleStream, config.comparator, config.limit);
  }

  /**
   * creates from explicit config
   */
  public IVrixSortStream(TupleStream innerStream, StreamComparator comparator, int limit) {
    init(innerStream, comparator, limit);
  }

  private void init(TupleStream innerStream, StreamComparator comparator, int limit) {
    this.processor = new LimitedSorter(comparator, limit, true);
    this.innerStream = innerStream;
    this.comparator = comparator;
    this.limit = limit;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return sortSyntax.toExpression(factory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return sortSyntax.toExplanation(factory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamComparator getStreamSort() {
    return comparator;
  }


  /**
   * Unfortunately, the syntax code in
   * {@link org.apache.solr.client.solrj.io.stream.RankStream}
   * cannot be re-used since it uses private modifiers...so that
   * code is copied and placed into this placeholder class.
   */
  private class SortSyntax {
    public class Config {
      public TupleStream tupleStream;
      public StreamComparator comparator;
      public int limit;
    }

    private final IVrixSortStream iVrixSortStream;

    public SortSyntax(IVrixSortStream iVrixSortStream) {
      this.iVrixSortStream = iVrixSortStream;
    }

    private Config extractConfig(StreamExpression expression, StreamFactory factory) throws IOException {
      // grab all parameters out
      List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
      StreamExpressionNamedParameter nParam = factory.getNamedOperand(expression, "n");
      StreamExpressionNamedParameter sortExpression = factory.getNamedOperand(expression, SORT);

      // validate expression contains only what we want.
      if(expression.getParameters().size() != streamExpressions.size() + 2){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
      }

      if(null == nParam || null == nParam.getParameter() || !(nParam.getParameter() instanceof StreamExpressionValue)){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single 'n' parameter of type positive integer but didn't find one",expression));
      }
      String nStr = ((StreamExpressionValue)nParam.getParameter()).getValue();
      int nInt = 0;
      try{
        nInt = Integer.parseInt(nStr);
        if(nInt <= 0){
          throw new IOException(String.format(Locale.ROOT,"invalid expression %s - topN '%s' must be greater than 0.",expression, nStr));
        }
      }
      catch(NumberFormatException e){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - topN '%s' is not a valid integer.",expression, nStr));
      }
      if(1 != streamExpressions.size()){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
      }
      if(null == sortExpression || !(sortExpression.getParameter() instanceof StreamExpressionValue)){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'over' parameter listing fields to unique over but didn't find one",expression));
      }

      TupleStream stream = factory.constructStream(streamExpressions.get(0));
      StreamComparator comp = factory.constructComparator(((StreamExpressionValue)sortExpression.getParameter()).getValue(), FieldComparator.class);

      Config config = new Config();
      config.tupleStream = stream;
      config.comparator = comp;
      config.limit = nInt;
      return config;
    }

    private StreamExpression toExpression(StreamFactory factory) throws IOException{
      return toExpression(factory, true);
    }

    private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
      // function name
      StreamExpression expression = new StreamExpression(factory.getFunctionName(iVrixSortStream.getClass()));

      // n
      expression.addParameter(new StreamExpressionNamedParameter("n", Integer.toString(iVrixSortStream.limit)));

      if(includeStreams){
        // stream
        if(innerStream instanceof Expressible){
          expression.addParameter(((Expressible)innerStream).toExpression(factory));
        }
        else{
          throw new IOException("This RankStream contains a non-expressible TupleStream - it cannot be converted to an expression");
        }
      }
      else{
        expression.addParameter("<stream>");
      }

      // sort
      expression.addParameter(new StreamExpressionNamedParameter(SORT, iVrixSortStream.comparator.toExpression(factory)));

      return expression;
    }

    private Explanation toExplanation(StreamFactory factory) throws IOException {

      return new StreamExplanation(getStreamNodeId().toString())
          .withChildren(new Explanation[]{
              innerStream.toExplanation(factory)
          })
          .withFunctionName(factory.getFunctionName(iVrixSortStream.getClass()))
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR)
          .withExpression(toExpression(factory, false).toString())
          .withHelper(iVrixSortStream.comparator.toExplanation(factory));
    }
  }
}
