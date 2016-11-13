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
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
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
      /*CountingCodelabSource source = new CountingCodelabSource(start, end);
      input.apply(org.apache.beam.sdk.io.Read.from(source));*/
      return input.apply(Create.of(1, 2, 3));
    }

    @Override
    public void validate(PBegin input) {
      checkArgument(start <= end, "Start must be less than or equal to End", start, end);
    }

  }
/*
  public class CountingCodelabSource extends BoundedSource {
    private final int start;
    private final int end;
    private CountingCodelabSource(int start, int end) {

    }

    @Override
    public List<? extends BoundedSource<T>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {

    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {

    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      // This Source does not produce kev/value pairs, so this is false. 
      return false;
    }
*/
     /**
     * Returns a new {@link BoundedReader} that reads from this source.
     */
     /*
    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {

    }


    @Override
    public void validate(PBegin input) {
      
    }
  }
    */

}