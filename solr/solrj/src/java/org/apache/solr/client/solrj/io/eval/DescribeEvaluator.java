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
package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class DescribeEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public DescribeEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(containedEvaluators.size() < 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least two values but found %d",expression,containedEvaluators.size()));
    }
  }

  public Object normalizeInputType(Object value) throws StreamEvaluatorException {
    Object normalized = super.normalizeInputType(value);
    
    if(null == normalized){
      return null;
    }
    else if(normalized instanceof BigDecimal){
      return normalized;
    }
    else if(normalized instanceof Collection){
      List<Object> flatList = new ArrayList<>();
      for(Object subValue : (List)normalized){
        if(subValue instanceof Collection){
          flatList.addAll((Collection)normalizeInputType(subValue));
        }
        else{
          flatList.add(normalizeInputType(subValue));
        }
      }
      
      return flatList;
    }
    else{
      throw new StreamEvaluatorException("Numeric value expected but found type %s for value %s", value.getClass().getName(), value.toString());
    }
  }  

  
  @Override
  public Object doWork(Object... values) throws IOException {
    // we know each value is a BigDecimal or a list of BigDecimals
    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
    Arrays.stream(values).mapToDouble(value -> ((BigDecimal)value).doubleValue()).forEach(value -> descriptiveStatistics.addValue(value));

    Map<String,Number> map = new HashMap<>();
    map.put("max", descriptiveStatistics.getMax());
    map.put("mean", descriptiveStatistics.getMean());
    map.put("min", descriptiveStatistics.getMin());
    map.put("stdev", descriptiveStatistics.getStandardDeviation());
    map.put("sum", descriptiveStatistics.getSum());
    map.put("N", descriptiveStatistics.getN());
    map.put("var", descriptiveStatistics.getVariance());
    map.put("kurtosis", descriptiveStatistics.getKurtosis());
    map.put("skewness", descriptiveStatistics.getSkewness());
    map.put("popVar", descriptiveStatistics.getPopulationVariance());
    map.put("geometricMean", descriptiveStatistics.getGeometricMean());
    map.put("sumsq", descriptiveStatistics.getSumsq());

    return new Tuple(map);
  }  
}
