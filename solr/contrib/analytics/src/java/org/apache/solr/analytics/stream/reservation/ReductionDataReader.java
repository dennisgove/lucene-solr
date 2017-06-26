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

import java.io.DataInput;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import org.apache.solr.analytics.util.function.BooleanConsumer;
import org.apache.solr.analytics.util.function.FloatConsumer;

/**
 * Abstract class to manage the reading and application of data from a {@link DataInput} stream.
 */
public abstract class ReductionDataReader<A> {
  protected final DataInput inputStream;
  protected final A applier;
  
  public ReductionDataReader(DataInput inputStream, A applier) {
    this.inputStream = inputStream;
    this.applier = applier;
  }
  
  /**
   * Read a piece of data from the input stream and feed it to the applier.
   * 
   * @throws IOException if an exception occurs while reading from the input stream
   */
  public abstract void read() throws IOException;
}
/**
 * Abstract class to manage the reading and application of data from a {@link DataInput} stream.
 * The data being read may not exist, so the reader first checks before reading.
 */
abstract class ReductionCheckedDataReader<A> extends ReductionDataReader<A> {
  
  public ReductionCheckedDataReader(DataInput inputStream, A applier) {
    super(inputStream, applier);
  }
  
  @Override
  /**
   * Read a piece of data from the input stream and feed it to the applier.
   * <br>
   * First checks that the piece of data exists before reading.
   * 
   * @throws IOException if an exception occurs while reading from the input stream
   */
  public void read() throws IOException {
    if (inputStream.readBoolean()) {
      checkedRead();
    }
  }
  
  /**
   * Read a piece of data from the input stream and feed it to the applier.
   * <br>
   * This piece of data is guaranteed to be there.
   * 
   * @throws IOException if an exception occurs while reading from the input stream
   */
  protected abstract void checkedRead() throws IOException;
}
/**
 * Abstract class to manage the reading and application of array data from a {@link DataInput} stream.
 */
abstract class ReductionDataArrayReader<A> extends ReductionDataReader<A> {
  protected final IntConsumer signal;
  
  public ReductionDataArrayReader(DataInput inputStream, A applier, IntConsumer signal) {
    super(inputStream, applier);
    
    this.signal = signal;
  }
  
  @Override
  /**
   * Read an array of data from the input stream and feed it to the applier, first signaling the size of the array.
   * 
   * @throws IOException if an exception occurs while reading from the input stream
   */
  public void read() throws IOException {
    int size = inputStream.readInt();
    signal.accept(size);
    read(size);
  }
  
  /**
   * Read an array from the input stream, feeding each member to the applier.
   * 
   * @param size length of the array to read
   * @throws IOException if an exception occurs while reading from the input stream
   */
  protected abstract void read(int size) throws IOException;
}
class BooleanDataReader extends ReductionDataReader<BooleanConsumer> {
  
  public BooleanDataReader(DataInput inputStream, BooleanConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void read() throws IOException {
    applier.accept(inputStream.readBoolean());
  }
}
class BooleanCheckedDataReader extends ReductionCheckedDataReader<BooleanConsumer> {
  
  public BooleanCheckedDataReader(DataInput inputStream, BooleanConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void checkedRead() throws IOException {
    applier.accept(inputStream.readBoolean());
  }
}
class BooleanDataArrayReader extends ReductionDataArrayReader<BooleanConsumer> {
  
  public BooleanDataArrayReader(DataInput inputStream, BooleanConsumer applier, IntConsumer signal) {
    super(inputStream, applier, signal);
  }
  @Override
  public void read(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      applier.accept(inputStream.readBoolean());
    }
  }
}
class IntDataReader extends ReductionDataReader<IntConsumer> {
  
  public IntDataReader(DataInput inputStream, IntConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void read() throws IOException {
    applier.accept(inputStream.readInt());
  }
}
class IntCheckedDataReader extends ReductionCheckedDataReader<IntConsumer> {
  
  public IntCheckedDataReader(DataInput inputStream, IntConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void checkedRead() throws IOException {
    applier.accept(inputStream.readInt());
  }
}
class IntDataArrayReader extends ReductionDataArrayReader<IntConsumer> {
  
  public IntDataArrayReader(DataInput inputStream, IntConsumer applier, IntConsumer signal) {
    super(inputStream, applier, signal);
  }
  @Override
  public void read(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      applier.accept(inputStream.readInt());
    }
  }
}
class LongDataReader extends ReductionDataReader<LongConsumer> {
  
  public LongDataReader(DataInput inputStream, LongConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void read() throws IOException {
    applier.accept(inputStream.readLong());
  }
}
class LongCheckedDataReader extends ReductionCheckedDataReader<LongConsumer> {
  
  public LongCheckedDataReader(DataInput inputStream, LongConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void checkedRead() throws IOException {
    applier.accept(inputStream.readLong());
  }
}
class LongDataArrayReader extends ReductionDataArrayReader<LongConsumer> {
  
  public LongDataArrayReader(DataInput inputStream, LongConsumer applier, IntConsumer signal) {
    super(inputStream, applier, signal);
  }
  @Override
  public void read(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      applier.accept(inputStream.readLong());
    }
  }
}
class FloatDataReader extends ReductionDataReader<FloatConsumer> {
  
  public FloatDataReader(DataInput inputStream, FloatConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void read() throws IOException {
    applier.accept(inputStream.readFloat());
  }
}
class FloatCheckedDataReader extends ReductionCheckedDataReader<FloatConsumer> {
  
  public FloatCheckedDataReader(DataInput inputStream, FloatConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void checkedRead() throws IOException {
    applier.accept(inputStream.readFloat());
  }
}
class FloatDataArrayReader extends ReductionDataArrayReader<FloatConsumer> {
  
  public FloatDataArrayReader(DataInput inputStream, FloatConsumer applier, IntConsumer signal) {
    super(inputStream, applier, signal);
  }
  @Override
  public void read(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      applier.accept(inputStream.readFloat());
    }
  }
}
class DoubleDataReader extends ReductionDataReader<DoubleConsumer> {
  
  public DoubleDataReader(DataInput inputStream, DoubleConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void read() throws IOException {
    applier.accept(inputStream.readDouble());
  }
}
class DoubleCheckedDataReader extends ReductionCheckedDataReader<DoubleConsumer> {
  
  public DoubleCheckedDataReader(DataInput inputStream, DoubleConsumer applier) {
    super(inputStream, applier);
  }
  @Override
  public void checkedRead() throws IOException {
    applier.accept(inputStream.readDouble());
  }
}
class DoubleDataArrayReader extends ReductionDataArrayReader<DoubleConsumer> {
  
  public DoubleDataArrayReader(DataInput inputStream, DoubleConsumer applier, IntConsumer signal) {
    super(inputStream, applier, signal);
  }
  @Override
  public void read(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      applier.accept(inputStream.readDouble());
    }
  }
}
class StringDataReader extends ReductionDataReader<Consumer<String>> {
  
  public StringDataReader(DataInput inputStream, Consumer<String> applier) {
    super(inputStream, applier);
  }
  @Override
  public void read() throws IOException {
    if (inputStream.readBoolean()) {
      applier.accept(inputStream.readUTF());
    }
  }
}
class StringCheckedDataReader extends ReductionCheckedDataReader<Consumer<String>> {
  
  public StringCheckedDataReader(DataInput inputStream, Consumer<String> applier) {
    super(inputStream, applier);
  }
  @Override
  public void checkedRead() throws IOException {
    applier.accept(inputStream.readUTF());
  }
}
class StringDataArrayReader extends ReductionDataArrayReader<Consumer<String>> {
  
  public StringDataArrayReader(DataInput inputStream, Consumer<String> applier, IntConsumer signal) {
    super(inputStream, applier, signal);
  }
  @Override
  public void read(int size) throws IOException {
    for (int i = 0; i < size; ++i) {
      applier.accept(inputStream.readUTF());
    }
  }
}