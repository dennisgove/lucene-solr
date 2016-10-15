/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.carrot2.shaded.guava.common.collect.Lists;

public class FilterStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private List<String> fqs;

  public FilterStream(TupleStream stream, String ... fq) throws IOException {
    init(stream, Arrays.asList(fq));
  }
  
  public FilterStream(TupleStream stream, List<String> fqs) throws IOException {
    init(stream, fqs);
  }
  
  public FilterStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    List<StreamExpressionNamedParameter> fqExpressions = factory.getNamedOperands(expression, "fq");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + fqExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found in FilterStream", expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single stream but found %d (must be TupleStream types)",expression, streamExpressions.size()));
    }

    if(0 == fqExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one fq field but found %d",expression, streamExpressions.size()));
    }

    init(stream = factory.constructStream(streamExpressions.get(0)),
        fqExpressions.stream().map(parameter -> parameter.getParameter().toString()).collect(Collectors.toList())
        );
  }
  
  private void init(TupleStream stream, List<String> fqs){
    this.stream = stream;
    this.fqs = fqs;
  }
    
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    if(includeStreams){
      // stream
      if(stream instanceof Expressible){
        expression.addParameter(((Expressible)stream).toExpression(factory));
      }
      else{
        throw new IOException("This FilterStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }

    for(String fq : fqs){
      expression.addParameter(new StreamExpressionNamedParameter("fq", fq));
    }
    
    return expression;   
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    Explanation explanation = new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[]{
        stream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString());   
    
    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    return Lists.newArrayList(stream);
  }

  public void open() throws IOException {
    stream.open();
  }

  public void close() throws IOException {
    stream.close();
  }

  public Tuple read() throws IOException {
    Tuple tuple = stream.read();
    
    while(!tuple.EOF && !isMatch(tuple)){
      tuple = stream.read();
    }

    return tuple;
  }
  
  private boolean isMatch(Tuple tuple){
    for (String fq : fqs) {
      if ((fq != null) && (fq.trim().length() != 0)) {
        fq = macroExpander.expand(fq);
        final QParser fqp = QParser.getParser(fq, req);
        final Query filterQuery = fqp.getQuery();
        if (filterQuery != null) {
          queryAndFilters.add(filterQuery);
        }
      }
    }
  }
  
  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return stream.getStreamSort();
  }

  public int getCost() {
    return 0;
  }
}