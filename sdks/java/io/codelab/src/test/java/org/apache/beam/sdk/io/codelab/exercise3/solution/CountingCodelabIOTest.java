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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of {@link CountingCodelabIO}.
 */
@RunWith(JUnit4.class)
public class CountingCodelabIOTest {


  @Test
  @Category(RunnableOnService.class)
  public void testSimpleRead() {
    Pipeline p = TestPipeline.create();
    PCollection<Integer> input = p.apply(CountingCodelabIO.read());

    long numElements = 99l;
    Integer start = 1;
    Integer end = 100;

    // Count == numElements
    PAssert.that(input.apply("Count", Count.<Integer>globally()))
            .containsInAnyOrder(numElements); // Note that the constant checked here must be a long.

    // Count of Unique Elements == numElements
    PAssert.that(input
                    .apply(Distinct.<Integer>create())
                    .apply("UniqueCount", Count.<Integer>globally()))
            .containsInAnyOrder(numElements);

    // Min == 0
    PAssert.that(input.apply("Min", Min.<Integer>globally())).containsInAnyOrder(start);

    // Max == numElements-1
    PAssert.that(input.apply("Max", Max.<Integer>globally()))
            .containsInAnyOrder(end-1);

    /* This is a style that we are trying to discourage new devs from using - it relies on side inputs, and thus is
       compatible with fewer runners.
    PAssert.thatSingleton(input.apply("Count2", Count.<Integer>globally()))
            .isEqualTo(100l);
    */
    p.run();
  }

  @Test
  public void testWithOptions() {
    // TODO
  }

  @Test
  public void testSourceBasic() {
    int start = 5;
    int end = 10;
    CountingCodelabIO.CountingCodelabSource source = new CountingCodelabIO.CountingCodelabSource(start, end);

    // TODO
  }

}