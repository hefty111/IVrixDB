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

import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.*;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.metrics.*;
import org.apache.solr.ivrixdb.search.stream.nonstream.NonStreamProcessorStream;
import org.apache.solr.ivrixdb.search.stream.nonstream.processor.Roller;

/**
 * This stream replaces {@link org.apache.solr.client.solrj.io.stream.RollupStream}
 * by implements rollup without relying on the sort order of the
 * underlying stream, and adds previewing capability. It creates and
 * holds buckets in-memory, and then returns them in a new sort
 * order. Note that there is a limit to the number of buckets.
 *
 * @author Ivri Faitelson
 */
public class IVrixRollupStream extends NonStreamProcessorStream {
  private final RollupSyntax rollupSyntax = new RollupSyntax(this);
  private Bucket[] bucketsConfig;
  private Metric[] metricsConfig;

  /**
   * creates from stream expression
   */
  public IVrixRollupStream(StreamExpression expression, StreamFactory factory) throws IOException {
    RollupSyntax.Config config = rollupSyntax.extractConfig(expression, factory);
    init(config.tupleStream, config.bucketsConfig, config.metricsConfig);
  }

  /**
   * creates from explicit config
   */
  public IVrixRollupStream(TupleStream tupleStream,
                           Bucket[] buckets,
                           Metric[] metrics) {
    init(tupleStream, buckets, metrics);
  }

  private void init(TupleStream tupleStream, Bucket[] buckets, Metric[] metrics) {
    this.processor = new Roller(buckets, metrics, true);
    this.innerStream = tupleStream;
    this.bucketsConfig = buckets;
    this.metricsConfig = metrics;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return rollupSyntax.toExpression(factory, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return rollupSyntax.toExplanation(factory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StreamComparator getStreamSort() {
    return innerStream.getStreamSort();
  }

  /**
   * Unfortunately, the syntax code in
   * {@link org.apache.solr.client.solrj.io.stream.RollupStream}
   * cannot be re-used since it uses private modifiers...so that
   * code is copied and placed into this placeholder class.
   */
  private class RollupSyntax {
    public class Config {
      public TupleStream tupleStream;
      public Bucket[] bucketsConfig;
      public Metric[] metricsConfig;
    }

    private final IVrixRollupStream iVrixRollupStream;

    public RollupSyntax(IVrixRollupStream iVrixRollupStream) {
      this.iVrixRollupStream = iVrixRollupStream;
    }

    public Config extractConfig(StreamExpression expression, StreamFactory factory) throws IOException {
      // grab all parameters out
      List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
      List<StreamExpression> metricExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class);
      StreamExpressionNamedParameter overExpression = factory.getNamedOperand(expression, "over");

      // validate expression contains only what we want.
      if(expression.getParameters().size() != streamExpressions.size() + metricExpressions.size() + 1){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
      }

      if(1 != streamExpressions.size()){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
      }

      if(null == overExpression || !(overExpression.getParameter() instanceof StreamExpressionValue)){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'over' parameter listing fields to rollup by but didn't find one",expression));
      }

      // Construct the metrics
      Metric[] metrics = new Metric[metricExpressions.size()];
      for(int idx = 0; idx < metricExpressions.size(); ++idx){
        metrics[idx] = factory.constructMetric(metricExpressions.get(idx));
      }

      // Construct the buckets.
      // Buckets are nothing more than equalitors (I think). We can use equalitors as helpers for creating the buckets, but because
      // I feel I'm missing something wrt buckets I don't want to change the use of buckets in this class to instead be equalitors.
      StreamEqualitor streamEqualitor = factory.constructEqualitor(((StreamExpressionValue)overExpression.getParameter()).getValue(), FieldEqualitor.class);
      List<FieldEqualitor> flattenedEqualitors = flattenEqualitor(streamEqualitor);
      Bucket[] buckets = new Bucket[flattenedEqualitors.size()];
      for(int idx = 0; idx < flattenedEqualitors.size(); ++idx){
        buckets[idx] = new Bucket(flattenedEqualitors.get(idx).getLeftFieldName());
        // while we're using equalitors we don't support those of the form a=b. Only single field names.
      }

      Config config = new Config();
      config.tupleStream = factory.constructStream(streamExpressions.get(0));
      config.bucketsConfig = buckets;
      config.metricsConfig = metrics;
      return config;
    }

    public List<FieldEqualitor> flattenEqualitor(StreamEqualitor equalitor){
      List<FieldEqualitor> flattenedList = new ArrayList<>();

      if(equalitor instanceof FieldEqualitor){
        flattenedList.add((FieldEqualitor)equalitor);
      }
      else if(equalitor instanceof MultipleFieldEqualitor){
        MultipleFieldEqualitor mEqualitor = (MultipleFieldEqualitor)equalitor;
        for(StreamEqualitor subEqualitor : mEqualitor.getEqs()){
          flattenedList.addAll(flattenEqualitor(subEqualitor));
        }
      }

      return flattenedList;
    }

    public StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
      // function name
      StreamExpression expression = new StreamExpression(factory.getFunctionName(iVrixRollupStream.getClass()));

      // stream
      if(includeStreams){
        expression.addParameter(((Expressible)innerStream).toExpression(factory));
      }
      else{
        expression.addParameter("<stream>");
      }

      // over
      StringBuilder overBuilder = new StringBuilder();
      for(Bucket bucket : bucketsConfig){
        if(overBuilder.length() > 0){ overBuilder.append(","); }
        overBuilder.append(bucket.toString());
      }
      expression.addParameter(new StreamExpressionNamedParameter("over",overBuilder.toString()));

      // metrics
      for(Metric metric : metricsConfig){
        expression.addParameter(metric.toExpression(factory));
      }

      return expression;
    }

    public StreamExpression toExpression(StreamFactory factory) throws IOException{
      return toExpression(factory, true);
    }

    public Explanation toExplanation(StreamFactory factory) throws IOException {
      Explanation explanation = new StreamExplanation(getStreamNodeId().toString())
          .withChildren(new Explanation[]{
              ((Expressible)innerStream).toExplanation(factory)
          })
          .withFunctionName(factory.getFunctionName(iVrixRollupStream.getClass()))
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(ExpressionType.STREAM_DECORATOR)
          .withExpression(toExpression(factory, false).toString());

      for(Metric metric : metricsConfig){
        explanation.withHelper(metric.toExplanation(factory));
      }

      return explanation;
    }
  }
}
