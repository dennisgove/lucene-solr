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
package org.apache.solr.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.carrot2.shaded.guava.common.collect.Lists;

public class LuceneMatchStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private List<Query> queries;

  public LuceneMatchStream(TupleStream stream, String ... fq) throws IOException {
    init(stream, Arrays.asList(fq));
  }
  
  public LuceneMatchStream(TupleStream stream, List<String> fqs) throws IOException {
    init(stream, fqs);
  }
  
  public LuceneMatchStream(StreamExpression expression,StreamFactory factory) throws IOException {
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
  
  private void init(TupleStream stream, List<String> fqs) throws IOException{
    this.stream = stream;
    this.queries = constructQueries(fqs);
  }
  
  private List<Query> constructQueries(List<String> fqs) throws IOException {
    Analyzer analyzer = new StandardAnalyzer();
    QueryParser parser = new QueryParser(null, analyzer); // default field is null
    
    List<Query> queries = new ArrayList<Query>(fqs.size());
    for(String fq : fqs){
      try{
        queries.add(parser.parse(fq));
      }
      catch(ParseException e){
        throw new IOException(String.format(Locale.ROOT, "Failed to parse fq '%s' with error %s", fq, e.getMessage()), e);
      }
    }
    return queries;
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

    for(String fq : queries.stream().map(item -> item.toString(null)).collect(Collectors.toList())){
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
    MemoryIndex index = MemoryIndex.fromDocument(createDocument(tuple), new StandardAnalyzer());
    
    for(Query query : queries){
      if(0 == index.search(query)){
        return false;
      }
    }
    
    return true;
//    return queries.stream().allMatch(query -> index.search(query) > 0);
  }
  
  private Document createDocument(Tuple tuple){
    Document document = new Document();
    
    for(Object key : tuple.fields.keySet()){
      String fieldName = (String)key;
      Object value = tuple.fields.get(key);
      
      Field field;
      if(value instanceof String){
        field = new StringField(fieldName, (String)value, Store.NO);
        // we're not handling text fields....
      }
      else if(value instanceof Float){
        field = new FloatPoint(fieldName, (Float)value);
      }
      else if(value instanceof Double){
        field = new DoublePoint(fieldName, (Double)value);
      }
      else if(value instanceof Long){
        field = new LongPoint(fieldName, (Long)value);
      }
      else if(value instanceof Integer){
        field = new IntPoint(fieldName, (Integer)value);
      }
      else{
        // value is not a known, supported type
        continue;
      }
      
      document.add(field);
    }
    
    return document;
  }
  
  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return stream.getStreamSort();
  }

  public int getCost() {
    return 0;
  }
}