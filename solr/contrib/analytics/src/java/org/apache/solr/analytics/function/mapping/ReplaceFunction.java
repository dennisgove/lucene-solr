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
package org.apache.solr.analytics.function.mapping;

import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.ExpressionFactory.CreatorFunction;
import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.util.function.FloatConsumer;
import org.apache.solr.analytics.values.AnalyticsValue;
import org.apache.solr.analytics.values.AnalyticsValueStream;
import org.apache.solr.analytics.values.BooleanValue;
import org.apache.solr.analytics.values.BooleanValueStream;
import org.apache.solr.analytics.values.DateValue;
import org.apache.solr.analytics.values.DateValueStream;
import org.apache.solr.analytics.values.DoubleValue;
import org.apache.solr.analytics.values.DoubleValueStream;
import org.apache.solr.analytics.values.FloatValue;
import org.apache.solr.analytics.values.FloatValueStream;
import org.apache.solr.analytics.values.IntValue;
import org.apache.solr.analytics.values.IntValueStream;
import org.apache.solr.analytics.values.LongValue;
import org.apache.solr.analytics.values.LongValueStream;
import org.apache.solr.analytics.values.StringValue;
import org.apache.solr.analytics.values.StringValueStream;
import org.apache.solr.analytics.values.AnalyticsValue.AbstractAnalyticsValue;
import org.apache.solr.analytics.values.BooleanValue.AbstractBooleanValue;
import org.apache.solr.analytics.values.BooleanValueStream.AbstractBooleanValueStream;
import org.apache.solr.analytics.values.DateValue.AbstractDateValue;
import org.apache.solr.analytics.values.DateValueStream.AbstractDateValueStream;
import org.apache.solr.analytics.values.DoubleValue.AbstractDoubleValue;
import org.apache.solr.analytics.values.DoubleValueStream.AbstractDoubleValueStream;
import org.apache.solr.analytics.values.FloatValue.AbstractFloatValue;
import org.apache.solr.analytics.values.FloatValueStream.AbstractFloatValueStream;
import org.apache.solr.analytics.values.IntValue.AbstractIntValue;
import org.apache.solr.analytics.values.IntValueStream.AbstractIntValueStream;
import org.apache.solr.analytics.values.LongValue.AbstractLongValue;
import org.apache.solr.analytics.values.LongValueStream.AbstractLongValueStream;
import org.apache.solr.analytics.values.StringValue.AbstractStringValue;
import org.apache.solr.analytics.values.StringValueStream.AbstractStringValueStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/**
 * A mapping function to replace an {@link AnalyticsValue} from an {@link AnalyticsValue} or an {@link AnalyticsValueStream}
 * with a different {@link AnalyticsValue}.
 * For each document, all values from the base parameter matching the comparison parameter will be replaced with the fill parameter.
 * <p>
 * The first parameter can be any type of analytics expression. If the parameter is multi-valued, then the return will be multi-valued. (Required)
 * <br>
 * The second parameter, which is the value to compare to the first parameter, must be an {@link AnalyticsValue}, aka single-valued. (Required)
 * <br>
 * The third parameter, which is the value to fill the first parameter with, must be an {@link AnalyticsValue}, aka single-valued. (Required)
 * <p>
 * The resulting Value or ValueStream will be typed with the closest super-type of the three parameters.
 * (e.g. {@value #name}(double,int,float) will return a double)
 */
public class ReplaceFunction {
  public static final String name = "replace";

  public static final CreatorFunction creatorFunction = (params -> {
    if (params.length != 3) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires 3 paramaters, " + params.length + " found.");
    }
    if (!(params[1] instanceof AnalyticsValue && params[2] instanceof AnalyticsValue)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,"The "+name+" function requires the comparator and fill parameters to be single-valued.");
    }

    AnalyticsValueStream baseExpr = params[0];
    AnalyticsValue compExpr = (AnalyticsValue)params[1];
    AnalyticsValue fillExpr = (AnalyticsValue)params[2];

    if (baseExpr instanceof DateValue && compExpr instanceof DateValue && fillExpr instanceof DateValue) {
      return new DateReplaceFunction((DateValue)baseExpr,(DateValue)compExpr,(DateValue)fillExpr);
    }
    if (baseExpr instanceof DateValueStream && compExpr instanceof DateValue && fillExpr instanceof DateValue) {
      return new DateStreamReplaceFunction((DateValueStream)baseExpr,(DateValue)compExpr,(DateValue)fillExpr);
    }
    if (baseExpr instanceof BooleanValue && compExpr instanceof BooleanValue && fillExpr instanceof BooleanValue) {
      return new BooleanReplaceFunction((BooleanValue)baseExpr,(BooleanValue)compExpr,(BooleanValue)fillExpr);
    }
    if (baseExpr instanceof BooleanValueStream && compExpr instanceof BooleanValue && fillExpr instanceof BooleanValue) {
      return new BooleanStreamReplaceFunction((BooleanValueStream)baseExpr,(BooleanValue)compExpr,(BooleanValue)fillExpr);
    }
    if (baseExpr instanceof IntValue && compExpr instanceof IntValue && fillExpr instanceof IntValue) {
      return new IntReplaceFunction((IntValue)baseExpr,(IntValue)compExpr,(IntValue)fillExpr);
    }
    if (baseExpr instanceof IntValueStream && compExpr instanceof IntValue && fillExpr instanceof IntValue) {
      return new IntStreamReplaceFunction((IntValueStream)baseExpr,(IntValue)compExpr,(IntValue)fillExpr);
    }
    if (baseExpr instanceof LongValue && compExpr instanceof LongValue && fillExpr instanceof LongValue) {
      return new LongReplaceFunction((LongValue)baseExpr,(LongValue)compExpr,(LongValue)fillExpr);
    }
    if (baseExpr instanceof LongValueStream && compExpr instanceof LongValue && fillExpr instanceof LongValue) {
      return new LongStreamReplaceFunction((LongValueStream)baseExpr,(LongValue)compExpr,(LongValue)fillExpr);
    }
    if (baseExpr instanceof FloatValue && compExpr instanceof FloatValue && fillExpr instanceof FloatValue) {
      return new FloatReplaceFunction((FloatValue)baseExpr,(FloatValue)compExpr,(FloatValue)fillExpr);
    }
    if (baseExpr instanceof FloatValueStream && compExpr instanceof FloatValue && fillExpr instanceof FloatValue) {
      return new FloatStreamReplaceFunction((FloatValueStream)baseExpr,(FloatValue)compExpr,(FloatValue)fillExpr);
    }
    if (baseExpr instanceof DoubleValue && compExpr instanceof DoubleValue && fillExpr instanceof DoubleValue) {
      return new DoubleReplaceFunction((DoubleValue)baseExpr,(DoubleValue)compExpr,(DoubleValue)fillExpr);
    }
    if (baseExpr instanceof DoubleValueStream && compExpr instanceof DoubleValue && fillExpr instanceof DoubleValue) {
      return new DoubleStreamReplaceFunction((DoubleValueStream)baseExpr,(DoubleValue)compExpr,(DoubleValue)fillExpr);
    }
    if (baseExpr instanceof StringValue && compExpr instanceof StringValue && fillExpr instanceof StringValue) {
      return new StringReplaceFunction((StringValue)baseExpr,(StringValue)compExpr,(StringValue)fillExpr);
    }
    if (baseExpr instanceof StringValueStream && compExpr instanceof StringValue && fillExpr instanceof StringValue) {
      return new StringStreamReplaceFunction((StringValueStream)baseExpr,(StringValue)compExpr,(StringValue)fillExpr);
    }
    if (baseExpr instanceof AnalyticsValue) {
      return new ValueReplaceFunction((AnalyticsValue)baseExpr,(AnalyticsValue)compExpr,(AnalyticsValue)fillExpr);
    }
    return new StreamReplaceFunction(baseExpr,compExpr,fillExpr); 
    
  });
}
class StreamReplaceFunction implements AnalyticsValueStream {
  private final AnalyticsValueStream baseExpr;
  private final AnalyticsValue compExpr;
  private final AnalyticsValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public StreamReplaceFunction(AnalyticsValueStream baseExpr, AnalyticsValue compExpr, AnalyticsValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }

  @Override
  public void streamObjects(Consumer<Object> cons) {
    Object compValue = compExpr.getObject();
    if (compExpr.exists()) {
      Object fillValue = fillExpr.getObject();
      boolean fillExists = fillExpr.exists();
      baseExpr.streamObjects(value -> {
        if (value.equals(compValue)) {
          if (fillExists) {
            cons.accept(fillValue);
          }
        } else {
          cons.accept(value);
        }
      });
    }
    else {
      baseExpr.streamObjects(cons);
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class ValueReplaceFunction extends AbstractAnalyticsValue {
  private final AnalyticsValue baseExpr;
  private final AnalyticsValue compExpr;
  private final AnalyticsValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public ValueReplaceFunction(AnalyticsValue baseExpr, AnalyticsValue compExpr, AnalyticsValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }
  
  boolean exists = false;

  @Override
  public Object getObject() {
    Object value = baseExpr.getObject();
    exists = baseExpr.exists();
    Object comp = compExpr.getObject();
    if (value.equals(comp) && exists==compExpr.exists()) {
      value = fillExpr.getObject();
      exists = fillExpr.exists();
    }
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class BooleanStreamReplaceFunction extends AbstractBooleanValueStream {
  private final BooleanValueStream baseExpr;
  private final BooleanValue compExpr;
  private final BooleanValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public BooleanStreamReplaceFunction(BooleanValueStream baseExpr, BooleanValue compExpr, BooleanValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }

  @Override
  public void streamBooleans(BooleanConsumer cons) {
    boolean compValue = compExpr.getBoolean();
    if (compExpr.exists()) {
      boolean fillValue = fillExpr.getBoolean();
      boolean fillExists = fillExpr.exists();
      baseExpr.streamBooleans(value -> {
        if (value==compValue) {
          if (fillExists) {
            cons.accept(fillValue);
          }
        } else {
          cons.accept(value);
        }
      });
    }
    else {
      baseExpr.streamBooleans(cons);
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class BooleanReplaceFunction extends AbstractBooleanValue {
  private final BooleanValue baseExpr;
  private final BooleanValue compExpr;
  private final BooleanValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public BooleanReplaceFunction(BooleanValue baseExpr, BooleanValue compExpr, BooleanValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }
  
  boolean exists = false;

  @Override
  public boolean getBoolean() {
    boolean value = baseExpr.getBoolean();
    exists = baseExpr.exists();
    boolean comp = compExpr.getBoolean();
    if (value==comp && exists==compExpr.exists()) {
      value = fillExpr.getBoolean();
      exists = fillExpr.exists();
    }
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class IntStreamReplaceFunction extends AbstractIntValueStream {
  private final IntValueStream baseExpr;
  private final IntValue compExpr;
  private final IntValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public IntStreamReplaceFunction(IntValueStream baseExpr, IntValue compExpr, IntValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }

  @Override
  public void streamInts(IntConsumer cons) {
    int compValue = compExpr.getInt();
    if (compExpr.exists()) {
      int fillValue = fillExpr.getInt();
      boolean fillExists = fillExpr.exists();
      baseExpr.streamInts(value -> {
        if (value==compValue) {
          if (fillExists) {
            cons.accept(fillValue);
          }
        } else {
          cons.accept(value);
        }
      });
    }
    else {
      baseExpr.streamInts(cons);
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class IntReplaceFunction extends AbstractIntValue {
  private final IntValue baseExpr;
  private final IntValue compExpr;
  private final IntValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public IntReplaceFunction(IntValue baseExpr, IntValue compExpr, IntValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }
  
  boolean exists = false;

  @Override
  public int getInt() {
    int value = baseExpr.getInt();
    exists = baseExpr.exists();
    int comp = compExpr.getInt();
    if (value==comp && exists==compExpr.exists()) {
      value = fillExpr.getInt();
      exists = fillExpr.exists();
    }
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class LongStreamReplaceFunction extends AbstractLongValueStream {
  private final LongValueStream baseExpr;
  private final LongValue compExpr;
  private final LongValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public LongStreamReplaceFunction(LongValueStream baseExpr, LongValue compExpr, LongValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    long compValue = compExpr.getLong();
    if (compExpr.exists()) {
      long fillValue = fillExpr.getLong();
      boolean fillExists = fillExpr.exists();
      baseExpr.streamLongs(value -> {
        if (value==compValue) {
          if (fillExists) {
            cons.accept(fillValue);
          }
        } else {
          cons.accept(value);
        }
      });
    }
    else {
      baseExpr.streamLongs(cons);
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class LongReplaceFunction extends AbstractLongValue {
  private final LongValue baseExpr;
  private final LongValue compExpr;
  private final LongValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public LongReplaceFunction(LongValue baseExpr, LongValue compExpr, LongValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }
  
  boolean exists = false;

  @Override
  public long getLong() {
    long value = baseExpr.getLong();
    exists = baseExpr.exists();
    long comp = compExpr.getLong();
    if (value==comp && exists==compExpr.exists()) {
      value = fillExpr.getLong();
      exists = fillExpr.exists();
    }
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class FloatStreamReplaceFunction extends AbstractFloatValueStream {
  private final FloatValueStream baseExpr;
  private final FloatValue compExpr;
  private final FloatValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public FloatStreamReplaceFunction(FloatValueStream baseExpr, FloatValue compExpr, FloatValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }

  @Override
  public void streamFloats(FloatConsumer cons) {
    float compValue = compExpr.getFloat();
    if (compExpr.exists()) {
      float fillValue = fillExpr.getFloat();
      boolean fillExists = fillExpr.exists();
      baseExpr.streamFloats(value -> {
        if (value==compValue) {
          if (fillExists) {
            cons.accept(fillValue);
          }
        } else {
          cons.accept(value);
        }
      });
    }
    else {
      baseExpr.streamFloats(cons);
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class FloatReplaceFunction extends AbstractFloatValue {
  private final FloatValue baseExpr;
  private final FloatValue compExpr;
  private final FloatValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public FloatReplaceFunction(FloatValue baseExpr, FloatValue compExpr, FloatValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }
  
  boolean exists = false;

  @Override
  public float getFloat() {
    float value = baseExpr.getFloat();
    exists = baseExpr.exists();
    float comp = compExpr.getFloat();
    if (value==comp && exists==compExpr.exists()) {
      value = fillExpr.getFloat();
      exists = fillExpr.exists();
    }
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DoubleStreamReplaceFunction extends AbstractDoubleValueStream {
  private final DoubleValueStream baseExpr;
  private final DoubleValue compExpr;
  private final DoubleValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public DoubleStreamReplaceFunction(DoubleValueStream baseExpr, DoubleValue compExpr, DoubleValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }

  @Override
  public void streamDoubles(DoubleConsumer cons) {
    double compValue = compExpr.getDouble();
    if (compExpr.exists()) {
      double fillValue = fillExpr.getDouble();
      boolean fillExists = fillExpr.exists();
      baseExpr.streamDoubles(value -> {
        if (value==compValue) {
          if (fillExists) {
            cons.accept(fillValue);
          }
        } else {
          cons.accept(value);
        }
      });
    }
    else {
      baseExpr.streamDoubles(cons);
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DoubleReplaceFunction extends AbstractDoubleValue {
  private final DoubleValue baseExpr;
  private final DoubleValue compExpr;
  private final DoubleValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public DoubleReplaceFunction(DoubleValue baseExpr, DoubleValue compExpr, DoubleValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }
  
  boolean exists = false;

  @Override
  public double getDouble() {
    double value = baseExpr.getDouble();
    exists = baseExpr.exists();
    double comp = compExpr.getDouble();
    if (value==comp && exists==compExpr.exists()) {
      value = fillExpr.getDouble();
      exists = fillExpr.exists();
    }
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DateStreamReplaceFunction extends AbstractDateValueStream {
  private final DateValueStream baseExpr;
  private final DateValue compExpr;
  private final DateValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public DateStreamReplaceFunction(DateValueStream baseExpr, DateValue compExpr, DateValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }

  @Override
  public void streamLongs(LongConsumer cons) {
    long compValue = compExpr.getLong();
    if (compExpr.exists()) {
      long fillValue = fillExpr.getLong();
      boolean fillExists = fillExpr.exists();
      baseExpr.streamLongs(value -> {
        if (value==compValue) {
          if (fillExists) {
            cons.accept(fillValue);
          }
        } else {
          cons.accept(value);
        }
      });
    }
    else {
      baseExpr.streamLongs(cons);
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class DateReplaceFunction extends AbstractDateValue {
  private final DateValue baseExpr;
  private final DateValue compExpr;
  private final DateValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public DateReplaceFunction(DateValue baseExpr, DateValue compExpr, DateValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }
  
  boolean exists = false;

  @Override
  public long getLong() {
    long value = baseExpr.getLong();
    exists = baseExpr.exists();
    long comp = compExpr.getLong();
    if (value==comp && exists==compExpr.exists()) {
      value = fillExpr.getLong();
      exists = fillExpr.exists();
    }
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class StringStreamReplaceFunction extends AbstractStringValueStream {
  private final StringValueStream baseExpr;
  private final StringValue compExpr;
  private final StringValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public StringStreamReplaceFunction(StringValueStream baseExpr, StringValue compExpr, StringValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }

  @Override
  public void streamStrings(Consumer<String> cons) {
    String compValue = compExpr.toString();
    if (compExpr.exists()) {
      String fillValue = fillExpr.toString();
      boolean fillExists = fillExpr.exists();
      baseExpr.streamStrings(value -> {
        if (value.equals(compValue)) {
          if (fillExists) {
            cons.accept(fillValue);
          }
        } else {
          cons.accept(value);
        }
      });
    }
    else {
      baseExpr.streamStrings(cons);
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}
class StringReplaceFunction extends AbstractStringValue {
  private final StringValue baseExpr;
  private final StringValue compExpr;
  private final StringValue fillExpr;
  public static final String name = ReplaceFunction.name;
  private final String exprStr;
  private final ExpressionType funcType;
  
  public StringReplaceFunction(StringValue baseExpr, StringValue compExpr, StringValue fillExpr) throws SolrException {
    this.baseExpr = baseExpr;
    this.compExpr = compExpr;
    this.fillExpr = fillExpr;
    this.exprStr = AnalyticsValueStream.createExpressionString(name,baseExpr,compExpr,fillExpr);
    this.funcType = AnalyticsValueStream.determineMappingPhase(exprStr,baseExpr,compExpr,fillExpr);
  }
  
  boolean exists = false;

  @Override
  public String getString() {
    String value = baseExpr.getString();
    exists = baseExpr.exists();
    String comp = compExpr.getString();
    if (value.equals(comp) && exists==compExpr.exists()) {
      value = fillExpr.getString();
      exists = fillExpr.exists();
    }
    return value;
  }
  @Override
  public boolean exists() {
    return exists;
  }
  
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getExpressionStr() {
    return exprStr;
  }
  @Override
  public ExpressionType getExpressionType() {
    return funcType;
  }
}