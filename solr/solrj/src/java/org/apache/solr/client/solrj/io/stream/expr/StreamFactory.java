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
package org.apache.solr.client.solrj.io.stream.expr;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;

/**
 * Used to convert strings into stream expressions
 */
public class StreamFactory implements Serializable {
  
  private transient HashMap<String,String> collectionZkHosts;
  private transient HashMap<String,Class<?>> functionNames;
  private transient String defaultZkHost;
  
  public StreamFactory(){
    collectionZkHosts = new HashMap<String,String>();
    functionNames = new HashMap<String,Class<?>>();
  }
  
  public StreamFactory withCollectionZkHost(String collectionName, String zkHost){
    this.collectionZkHosts.put(collectionName, zkHost);
    return this;
  }

  public StreamFactory withDefaultZkHost(String zkHost) {
    this.defaultZkHost = zkHost;
    return this;
  }

  public String getDefaultZkHost() {
    return this.defaultZkHost;
  }

  public String getCollectionZkHost(String collectionName){
    if(this.collectionZkHosts.containsKey(collectionName)){
      return this.collectionZkHosts.get(collectionName);
    }
    return null;
  }
  
  public Map<String,Class<?>> getFunctionNames(){
    return functionNames;
  }
  public StreamFactory withFunctionName(String functionName, Class<?> clazz){
    this.functionNames.put(functionName, clazz);
    return this;
  }
  
  /**
   * Given an expression will return parameter at given index, or null if doesn't exist
   * @param expression expression to look at
   * @param atIndex index we want parameter for
   * @return parameter at index, else null
   */
  public StreamExpressionParameter getParameter(StreamExpression expression, int atIndex){
    if(null == expression.getParameters() || atIndex >= expression.getParameters().size()){
      return null;
    }
    
    return expression.getParameters().get(atIndex);
  }

  /**
   * Given an expression will return parameter at given index iff parameter is of the provided types
   * @param expression expression to look at
   * @param atIndex index we want parameter for
   * @param ofTypes types the parameter must be of (all must match)
   * @return parameter at index of types, else null
   */
  public StreamExpressionParameter getParameter(StreamExpression expression, int atIndex, Class<?> ... ofTypes){
    StreamExpressionParameter parameter = getParameter(expression, atIndex);
    if(isAssignableToAll(parameter, ofTypes)){
      return parameter;
    }
    
    return null;
  }
  
  /**
   * Given an expression will return all parameters of the provided types
   * @param expression expression to consider
   * @param ofTypes types each parameter must be assignable to (all must match)
   * @return list of all parameters matching the types
   */
  public List<StreamExpressionParameter> getParameters(StreamExpression expression, Class<?> ... ofTypes){
    return toList(expression.getParameters().stream().filter(item -> isAssignableToAll(item, ofTypes)));
  }

  /**
   * Given an expression will return the value parameter at the given index, or null if doesn't exist
   * @param expression expression to consider
   * @param atIndex index to find value parameter at
   * @return value parameter at index, null if not found
   */
  public String getValueParameter(StreamExpression expression, int atIndex){
    StreamExpressionParameter parameter = getParameter(expression, atIndex, StreamExpressionValue.class);
    if(null != parameter){ 
      return ((StreamExpressionValue)parameter).getValue();
    }
    
    return null;
  }
  
  /**
   * Given an expression will return all named parameters
   * @param expression expression to consider
   * @return all named parameters in the expression
   */
  public List<StreamExpressionNamedParameter> getNamedParameters(StreamExpression expression){
    return toList(getParameters(expression, StreamExpressionNamedParameter.class).stream().map(StreamExpressionNamedParameter.class::cast));
  }

  /**
   * Given an expression will return all parameter with the given name
   * @param expression expression to consider
   * @param withName parameter name to find
   * @return all parameters with given name (empty list of none found)
   */
  public List<StreamExpressionParameter> getParameters(StreamExpression expression, String withName){
    return toList(getNamedParameters(expression).stream().filter(item -> item.getName().equals(withName)).map(StreamExpressionParameter.class::cast));
  }
  
  /**
   * Given an expression will return parameter with the given name
   * @param expression expression to consider
   * @param withName parameter name to find
   * @return parameter with given name else null if not found
   * @throws IOException if multiple parameters found with the given name 
   */
  public StreamExpressionParameter getParameter(StreamExpression expression, String withName) throws IOException{
    List<StreamExpressionParameter> namedParameters = getParameters(expression, withName);
    
    if(1 == namedParameters.size()){
      return namedParameters.get(0);
    }
    else if(namedParameters.size() > 1){
      // found more than 1 with name
      throw new IOException(String.format(Locale.ROOT, "Expected to find a single parameter with name '%s' but found %d", withName, namedParameters.size()));
    }
    else{
      // found none with name
      return null;
    }
  }
    
  /**
   * Given an expression will return all parameters which are themselves expressions
   * @param expression expression to consider
   * @return list of all parameters which are themselves expressions
   */
  public List<StreamExpression> getExpressionParameters(StreamExpression expression){
    return toList(getParameters(expression, StreamExpression.class).stream().map(StreamExpression.class::cast));
  }
  
  public List<StreamExpressionNamedParameter> getNamedExpressionOperands(StreamExpression expression){
    List<StreamExpressionNamedParameter> namedParameters = new ArrayList<StreamExpressionNamedParameter>();
    for(StreamExpressionParameter parameter : getParameters(expression, StreamExpressionNamedParameter.class)){
      namedParameters.add((StreamExpressionNamedParameter)parameter);
    }
    
    return namedParameters;
  }
  public List<StreamExpression> getExpressionOperands(StreamExpression expression, String functionName){
    List<StreamExpression> namedParameters = new ArrayList<StreamExpression>();
    for(StreamExpressionParameter parameter : getParameters(expression, StreamExpression.class)){
      StreamExpression expressionOperand = (StreamExpression)parameter;
      if(expressionOperand.getFunctionName().equals(functionName)){
        namedParameters.add(expressionOperand);
      }
    }
    
    return namedParameters;
  }
  
  public List<StreamExpression> getExpressionOperandsRepresentingTypes(StreamExpression expression, Class ... clazzes){
    List<StreamExpression> matchingStreamExpressions = new ArrayList<StreamExpression>();
    List<StreamExpression> allStreamExpressions = getExpressionParameters(expression);
    
    parameterLoop:
    for(StreamExpression streamExpression : allStreamExpressions){
      if(functionNames.containsKey(streamExpression.getFunctionName())){
        for(Class clazz : clazzes){
          if(!clazz.isAssignableFrom(functionNames.get(streamExpression.getFunctionName()))){
            continue parameterLoop;
          }
        }
        
        matchingStreamExpressions.add(streamExpression);
      }
    }
    
    return matchingStreamExpressions;   
  }
    
  public TupleStream constructStream(String expressionClause) throws IOException {
    return constructStream(StreamExpressionParser.parse(expressionClause));
  }
  public TupleStream constructStream(StreamExpression expression) throws IOException{
    String function = expression.getFunctionName();
    if(functionNames.containsKey(function)){
      Class clazz = functionNames.get(function);
      if(Expressible.class.isAssignableFrom(clazz) && TupleStream.class.isAssignableFrom(clazz)){
        TupleStream stream = (TupleStream)createInstance(functionNames.get(function), new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
        return stream;
      }
    }
    
    throw new IOException(String.format(Locale.ROOT,"Invalid stream expression %s - function '%s' is unknown (not mapped to a valid TupleStream)", expression, expression.getFunctionName()));
  }
  
  public Metric constructMetric(String expressionClause) throws IOException {
    return constructMetric(StreamExpressionParser.parse(expressionClause));
  }
  public Metric constructMetric(StreamExpression expression) throws IOException{
    String function = expression.getFunctionName();
    if(functionNames.containsKey(function)){
      Class clazz = functionNames.get(function);
      if(Expressible.class.isAssignableFrom(clazz) && Metric.class.isAssignableFrom(clazz)){
        Metric metric = (Metric)createInstance(functionNames.get(function), new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
        return metric;
      }
    }
    
    throw new IOException(String.format(Locale.ROOT,"Invalid metric expression %s - function '%s' is unknown (not mapped to a valid Metric)", expression, expression.getFunctionName()));
  }

  public StreamComparator constructComparator(String comparatorString, Class comparatorType) throws IOException {
    if(comparatorString.contains(",")){
      String[] parts = comparatorString.split(",");
      StreamComparator[] comps = new StreamComparator[parts.length];
      for(int idx = 0; idx < parts.length; ++idx){
        comps[idx] = constructComparator(parts[idx].trim(), comparatorType);
      }
      return new MultipleFieldComparator(comps);
    }
    else if(comparatorString.contains("=")){
      // expected format is "left=right order"
      String[] parts = comparatorString.split("[ =]");
      
      if(parts.length < 3){
        throw new IOException(String.format(Locale.ROOT,"Invalid comparator expression %s - expecting 'left=right order'",comparatorString));
      }
      
      String leftFieldName = null;
      String rightFieldName = null;
      String order = null;
      for(String part : parts){
        // skip empty
        if(null == part || 0 == part.trim().length()){ continue; }
        
        // assign each in order
        if(null == leftFieldName){ 
          leftFieldName = part.trim(); 
        }
        else if(null == rightFieldName){ 
          rightFieldName = part.trim(); 
        }
        else if(null == order){ 
          order = part.trim();
          break; // we're done, stop looping
        }
      }
      
      if(null == leftFieldName || null == rightFieldName || null == order){
        throw new IOException(String.format(Locale.ROOT,"Invalid comparator expression %s - expecting 'left=right order'",comparatorString));
      }
      
      return (StreamComparator)createInstance(comparatorType, new Class[]{ String.class, String.class, ComparatorOrder.class }, new Object[]{ leftFieldName, rightFieldName, ComparatorOrder.fromString(order) });
    }
    else{
      // expected format is "field order"
      String[] parts = comparatorString.split(" ");
      if(2 != parts.length){
        throw new IOException(String.format(Locale.ROOT,"Invalid comparator expression %s - expecting 'field order'",comparatorString));
      }
      
      String fieldName = parts[0].trim();
      String order = parts[1].trim();
      
      return (StreamComparator)createInstance(comparatorType, new Class[]{ String.class, ComparatorOrder.class }, new Object[]{ fieldName, ComparatorOrder.fromString(order) });
    }
  }
    
  public StreamEqualitor constructEqualitor(String equalitorString, Class equalitorType) throws IOException {
    if(equalitorString.contains(",")){
      String[] parts = equalitorString.split(",");
      StreamEqualitor[] eqs = new StreamEqualitor[parts.length];
      for(int idx = 0; idx < parts.length; ++idx){
        eqs[idx] = constructEqualitor(parts[idx].trim(), equalitorType);
      }
      return new MultipleFieldEqualitor(eqs);
    }
    else{
      String leftFieldName;
      String rightFieldName;
      
      if(equalitorString.contains("=")){
        String[] parts = equalitorString.split("=");
        if(2 != parts.length){
          throw new IOException(String.format(Locale.ROOT,"Invalid equalitor expression %s - expecting fieldName=fieldName",equalitorString));
        }
        
        leftFieldName = parts[0].trim();
        rightFieldName = parts[1].trim();
      }
      else{
        leftFieldName = rightFieldName = equalitorString.trim();
      }
      
      return (StreamEqualitor)createInstance(equalitorType, new Class[]{ String.class, String.class }, new Object[]{ leftFieldName, rightFieldName });
    }
  }
  
  public Metric constructOperation(String expressionClause) throws IOException {
    return constructMetric(StreamExpressionParser.parse(expressionClause));
  }
  public StreamOperation constructOperation(StreamExpression expression) throws IOException{
    String function = expression.getFunctionName();
    if(functionNames.containsKey(function)){
      Class clazz = functionNames.get(function);
      if(Expressible.class.isAssignableFrom(clazz) && StreamOperation.class.isAssignableFrom(clazz)){
        return (StreamOperation)createInstance(functionNames.get(function), new Class[]{ StreamExpression.class, StreamFactory.class }, new Object[]{ expression, this});
      }
    }
    
    throw new IOException(String.format(Locale.ROOT,"Invalid operation expression %s - function '%s' is unknown (not mapped to a valid StreamOperation)", expression, expression.getFunctionName()));
  }


  public <T> T createInstance(Class<T> clazz, Class<?>[] paramTypes, Object[] params) throws IOException{
    Constructor<T> ctor;
    try {
      ctor = clazz.getConstructor(paramTypes);
      return ctor.newInstance(params);
      
    } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      if(null != e.getMessage()){
        throw new IOException(String.format(Locale.ROOT,"Unable to construct instance of %s caused by %s", clazz.getName(), e.getMessage()),e);
      }
      else{
        throw new IOException(String.format(Locale.ROOT,"Unable to construct instance of %s", clazz.getName()),e);
      }
    }
  }
  
  public String getFunctionName(Class<?> clazz) throws IOException{
    for(Entry<String,Class<?>> entry : functionNames.entrySet()){
      if(entry.getValue() == clazz){
        return entry.getKey();
      }
    }
    
    throw new IOException(String.format(Locale.ROOT, "Unable to find function name for class '%s'", clazz.getName()));
  }
  
  public Object constructPrimitiveObject(String original){
    String lower = original.trim().toLowerCase(Locale.ROOT);
    
    if("null".equals(lower)){ return null; }
    if("true".equals(lower) || "false".equals(lower)){ return Boolean.parseBoolean(lower); }
    try{ return Long.valueOf(original); } catch(Exception e){};
    try{ if (original.matches(".{1,8}")){ return Float.valueOf(original); }} catch(Exception e){};
    try{ if (original.matches(".{1,17}")){ return Double.valueOf(original); }} catch(Exception e){};
    
    // is a string
    return original;
  }
  
  private <T> List<T> toList(Stream<T> stream){
    return stream.collect(Collectors.toList());
  }

  private boolean isAssignableToAll(Object item, Class ... clazzes){
    for(Class clazz : clazzes){
      if(!clazz.isAssignableFrom(item.getClass())){
        return false;
      }
    }
    
    return true;
  }
}
