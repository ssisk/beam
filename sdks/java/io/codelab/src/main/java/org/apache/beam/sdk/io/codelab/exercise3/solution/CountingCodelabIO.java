/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.codelab.exercise3.solution;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.coders.*;
import static org.apache.beam.sdk.coders.BigEndianIntegerCoder.*;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/*

Beam IO Code Lab - Exercise 3 Solution

In this exercise, you will create a Source that does initial splitting but not DWR splitting.

*/

public class CountingCodelabIO {

  public static CountingCodelabIO.Read read() {
    return new CountingCodelabIO.Read(1, 100);
  }

  private CountingCodelabIO() {}

  public static class Read extends PTransform<PBegin, PCollection<Integer>> {
    private final int start;
    private final int end;

    private Read(int start, int end) {
      this.start = start;
      this.end = end;
    }

    /*
     * Allows the user to specify a range of integers to return.
     */
    public CountingCodelabIO.Read withRange(int start, int end) {
      return new Read(start, end);
    }

    @Override
    public PCollection<Integer> apply(PBegin input) {
      CountingCodelabSource source = new CountingCodelabSource(start, end);
      return input.apply(org.apache.beam.sdk.io.Read.from(source));
    }

    @Override
    public void validate(PBegin input) {
      checkArgument(start <= end, "Start must be less than or equal to End", start, end);
    }

  }
  public static class CountingCodelabSource extends BoundedSource<Integer> {
    // TODO - this implementation is just a quick mock up - haven't thought through whether this meets correct
    // Source semantics.
    private final int start;
    private final int end;
    public CountingCodelabSource(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public List<? extends BoundedSource<Integer>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      int sizeOfInteger = 4;
      return (end - start) * sizeOfInteger;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      // This Source does not produce kev/value pairs, so this is false. 
      return false;
    }
     /**
     * Returns a new {@link BoundedReader} that reads from this source.
     */
    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
      return new CountingCodelabReader(this, start, end);
    }


    @Override
    public void validate() {
      // TODO
    }

    @Override
    public Coder getDefaultOutputCoder() {
      // TODO - should we use a better coder?
      return BigEndianIntegerCoder.of();
    }

    private static class CountingCodelabReader extends BoundedReader<Integer> {
      // TODO - this implementation is just a quick mock up - haven't thought through whether this meets correct
      // Reader semantics.
      private int current;
      private int start;
      private int end;
      private CountingCodelabSource source;

      public CountingCodelabReader(CountingCodelabSource source, int start, int end) {
        this.start = start;
        this.end = end;
        this.source = source;
      }

      @Override
      public boolean start() throws IOException {
        current = start;
        return true;
      }

      @Override
      public boolean advance() throws IOException {
        current++;
        return current < end;
      }

      @Override
      public Integer getCurrent() throws NoSuchElementException {
        if (current < end) {
            return current;
        }
        return null;
      }

      @Override
      public void close() throws IOException {

      }

      @Override
      public BoundedSource<Integer> getCurrentSource() {
        return source;
      }
    }
  }

}