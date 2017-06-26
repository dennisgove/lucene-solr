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
package org.apache.solr.analytics.stream.reservation;

import java.io.DataOutput;
import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.solr.analytics.util.function.FloatSupplier;

/**
 * Abstract class to manage the extraction and writing of data to a {@link DataOutput} stream.
 */
public abstract class ReductionDataWriter<E> {
  protected final DataOutput output;
  protected final E extractor;
  
  public ReductionDataWriter(DataOutput output, E extractor) {
    this.output = output;
    this.extractor = extractor;
  }

  /**
   * Write a piece of data, retrieved from the extractor, to the output stream.
   * 
   * @throws IOException if an exception occurs while writing to the output stream
   */
  public abstract void write() throws IOException;
}
/**
 * Abstract class to manage the extraction and writing of data to a {@link DataOutput} stream.
 * The data being written may not exist, so the writer first writes whether the data exists before writing the data.
 */
abstract class ReductionCheckedDataWriter<C> extends ReductionDataWriter<C> {
  private final BooleanSupplier existsSupplier;
  
  public ReductionCheckedDataWriter(DataOutput output, C extractor, BooleanSupplier existsSupplier) {
    super(output, extractor);
    
    this.existsSupplier = existsSupplier;
  }
  
  /**
   * Write a piece of data, retrieved from the extractor, to the output stream.
   * <br>
   * First writes whether the data exists, then if it does exists writes the data.
   * 
   * @throws IOException if an exception occurs while writing to the output stream
   */
  @Override
  public void write() throws IOException {
    boolean exists = existsSupplier.getAsBoolean();
    output.writeBoolean(exists);
    if (exists) {
      checkedWrite();
    }
  }
  
  /**
   * Write a piece of data, retrieved from the extractor, to the output stream.
   * <br>
   * The data being written is guaranteed to exist.
   * 
   * @throws IOException if an exception occurs while writing to the output stream
   */
  protected abstract void checkedWrite() throws IOException;
}
/**
 * Abstract class to manage the extraction and writing of array data to a {@link DataOutput} stream.
 */
abstract class ReductionDataArrayWriter<C> extends ReductionDataWriter<C> {
  private final IntSupplier sizeSupplier;
  
  public ReductionDataArrayWriter(DataOutput output, C extractor, IntSupplier sizeSupplier) {
    super(output, extractor);
    
    this.sizeSupplier = sizeSupplier;
  }
  
  /**
   * Write an array of data, retrieved from the extractor, and its size, received from the sizeSupplier, to the output stream.
   * 
   * @throws IOException if an exception occurs while writing to the output stream
   */
  @Override
  public void write() throws IOException {
    int size = sizeSupplier.getAsInt();
    output.writeInt(size);
    write(size);
  }
  
  /**
   * Write an array of data, retrieved from the extractor, with the given size to the output stream.
   * 
   * @throws IOException if an exception occurs while writing to the output stream
   */
  protected abstract void write(int size) throws IOException;
}
class BooleanDataWriter extends ReductionDataWriter<BooleanSupplier> {
  
  public BooleanDataWriter(DataOutput output, BooleanSupplier extractor) {
    super(output, extractor);
  }

  @Override
  public void write() throws IOException {
    output.writeBoolean(extractor.getAsBoolean());
  }
}
class BooleanCheckedDataWriter extends ReductionCheckedDataWriter<BooleanSupplier> {
  
  public BooleanCheckedDataWriter(DataOutput output, BooleanSupplier extractor, BooleanSupplier existsSupplier) {
    super(output, extractor, existsSupplier);
  }

  @Override
  public void checkedWrite() throws IOException {
    output.writeBoolean(extractor.getAsBoolean());
  }
}
class BooleanDataArrayWriter extends ReductionDataArrayWriter<BooleanSupplier> {

  public BooleanDataArrayWriter(DataOutput output, BooleanSupplier extractor, IntSupplier sizeSupplier) {
    super(output, extractor, sizeSupplier);
  }
  
  @Override
  public void write(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      output.writeBoolean(extractor.getAsBoolean());
    }
  }
}
class IntDataWriter extends ReductionDataWriter<IntSupplier> {
  
  public IntDataWriter(DataOutput output, IntSupplier extractor) {
    super(output, extractor);
  }

  @Override
  public void write() throws IOException {
    output.writeInt(extractor.getAsInt());
  }
}
class IntCheckedDataWriter extends ReductionCheckedDataWriter<IntSupplier> {
  
  public IntCheckedDataWriter(DataOutput output, IntSupplier extractor, BooleanSupplier existsSupplier) {
    super(output, extractor, existsSupplier);
  }

  @Override
  public void checkedWrite() throws IOException {
    output.writeInt(extractor.getAsInt());
  }
}
class IntDataArrayWriter extends ReductionDataArrayWriter<IntSupplier> {

  public IntDataArrayWriter(DataOutput output, IntSupplier extractor, IntSupplier sizeSupplier) {
    super(output, extractor, sizeSupplier);
  }
  
  @Override
  public void write(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      output.writeInt(extractor.getAsInt());
    }
  }
}
class LongDataWriter extends ReductionDataWriter<LongSupplier> {
  
  public LongDataWriter(DataOutput output, LongSupplier extractor) {
    super(output, extractor);
  }

  @Override
  public void write() throws IOException {
    output.writeLong(extractor.getAsLong());
  }
}
class LongCheckedDataWriter extends ReductionCheckedDataWriter<LongSupplier> {
  
  public LongCheckedDataWriter(DataOutput output, LongSupplier extractor, BooleanSupplier existsSupplier) {
    super(output, extractor, existsSupplier);
  }

  @Override
  public void checkedWrite() throws IOException {
    output.writeLong(extractor.getAsLong());
  }
}
class LongDataArrayWriter extends ReductionDataArrayWriter<LongSupplier> {

  public LongDataArrayWriter(DataOutput output, LongSupplier extractor, IntSupplier sizeSupplier) {
    super(output, extractor, sizeSupplier);
  }
  
  @Override
  public void write(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      output.writeLong(extractor.getAsLong());
    }
  }
}
class FloatDataWriter extends ReductionDataWriter<FloatSupplier> {
  
  public FloatDataWriter(DataOutput output, FloatSupplier extractor) {
    super(output, extractor);
  }

  @Override
  public void write() throws IOException {
    output.writeFloat(extractor.getAsFloat());
  }
}
class FloatCheckedDataWriter extends ReductionCheckedDataWriter<FloatSupplier> {
  
  public FloatCheckedDataWriter(DataOutput output, FloatSupplier extractor, BooleanSupplier existsSupplier) {
    super(output, extractor, existsSupplier);
  }

  @Override
  public void checkedWrite() throws IOException {
    output.writeFloat(extractor.getAsFloat());
  }
}
class FloatDataArrayWriter extends ReductionDataArrayWriter<FloatSupplier> {

  public FloatDataArrayWriter(DataOutput output, FloatSupplier extractor, IntSupplier sizeSupplier) {
    super(output, extractor, sizeSupplier);
  }
  
  @Override
  public void write(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      output.writeFloat(extractor.getAsFloat());
    }
  }
}
class DoubleDataWriter extends ReductionDataWriter<DoubleSupplier> {
  
  public DoubleDataWriter(DataOutput output, DoubleSupplier extractor) {
    super(output, extractor);
  }

  @Override
  public void write() throws IOException {
    output.writeDouble(extractor.getAsDouble());
  }
}
class DoubleCheckedDataWriter extends ReductionCheckedDataWriter<DoubleSupplier> {
  
  public DoubleCheckedDataWriter(DataOutput output, DoubleSupplier extractor, BooleanSupplier existsSupplier) {
    super(output, extractor, existsSupplier);
  }

  @Override
  public void checkedWrite() throws IOException {
    output.writeDouble(extractor.getAsDouble());
  }
}
class DoubleDataArrayWriter extends ReductionDataArrayWriter<DoubleSupplier> {

  public DoubleDataArrayWriter(DataOutput output, DoubleSupplier extractor, IntSupplier sizeSupplier) {
    super(output, extractor, sizeSupplier);
  }
  
  @Override
  public void write(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      output.writeDouble(extractor.getAsDouble());
    }
  }
}
class StringDataWriter extends ReductionDataWriter<Supplier<String>> {
  
  public StringDataWriter(DataOutput output, Supplier<String> extractor) {
    super(output, extractor);
  }

  @Override
  public void write() throws IOException {
    String temp = extractor.get();
    output.writeBoolean(temp != null);
    if (temp != null) {
      output.writeUTF(temp);
    }
  }
}
class StringCheckedDataWriter extends ReductionCheckedDataWriter<Supplier<String>> {
  
  public StringCheckedDataWriter(DataOutput output, Supplier<String> extractor, BooleanSupplier existsSupplier) {
    super(output, extractor, existsSupplier);
  }

  @Override
  public void checkedWrite() throws IOException {
    output.writeUTF(extractor.get());
  }
}
class StringDataArrayWriter extends ReductionDataArrayWriter<Supplier<String>> {

  public StringDataArrayWriter(DataOutput output, Supplier<String> extractor, IntSupplier sizeSupplier) {
    super(output, extractor, sizeSupplier);
  }
  
  @Override
  public void write(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      output.writeUTF(extractor.get());
    }
  }
}