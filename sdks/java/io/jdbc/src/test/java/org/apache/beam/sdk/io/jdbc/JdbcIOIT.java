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
package org.apache.beam.sdk.io.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.common.DataSetExpectedValues;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on an independent Postgres instance.
 *
 * <p>This test requires a running instance of Postgres, and the test dataset must exist in the
 * database. `JdbcTestDataSet` will create the read table.
 *
 * <p>You can run this test by doing the following:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/jdbc -DintegrationTestPipelineOptions='[
 *  "--postgresServerName=1.2.3.4",
 *  "--postgresUsername=postgres",
 *  "--postgresDatabaseName=myfancydb",
 *  "--postgresPassword=mypass",
 *  "--postgresSsl=false" ]'
 * </pre>
 *
 * <p>If you want to run this with a runner besides directrunner, there are profiles for dataflow
 * and spark in the jdbc pom. You'll want to activate those in addition to the normal test runner
 * invocation pipeline options.
 */
@RunWith(JUnit4.class)
public class JdbcIOIT {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcIOIT.class);
  private static PGSimpleDataSource dataSource;
  private static String tableName;

  private static DataSetExpectedValues dataSetExpectedValues;

  @BeforeClass
  public static void setup() throws SQLException {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    // We do dataSource set up in BeforeClass rather than Before since we don't need to create a new
    // dataSource for each test.
    dataSource = JdbcTestDataSet.getDataSource(options);
    dataSetExpectedValues = JdbcTestDataSet.DATA_SET_EXPECTATION_MAP.get(options.getDataSetSize());

    tableName = JdbcTestDataSet.getWriteTableName();
    JdbcTestDataSet.createDataTable(dataSource, tableName);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    // Only do write table clean up once for the class since we don't want to clean up after both
    // read and write tests, only want to do it once after all the tests are done.
    JdbcTestDataSet.cleanUpDataTable(dataSource, tableName);
  }

  private static class CreateKVOfNameAndId implements JdbcIO.RowMapper<KV<String, Integer>> {
    @Override
    public KV<String, Integer> mapRow(ResultSet resultSet) throws Exception {
      KV<String, Integer> kv =
          KV.of(resultSet.getString("name"), resultSet.getInt("id"));
      return kv;
    }
  }

  private static class SelectNameFn extends DoFn<KV<String, Integer>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey());
    }
  }


  private static class PutKeyInColumnOnePutValueInColumnTwo
      implements JdbcIO.PreparedStatementSetter<KV<Long, String>> {
    @Override
    public void setParameters(KV<Long, String> element, PreparedStatement statement)
                    throws SQLException {
      statement.setLong(1, element.getKey());
      statement.setString(2, element.getValue());
    }
  }

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  /**
   * Given a Long as a seed value, constructs a test data row used by the IT for testing writes.
   */
  private static class ConstructTestDataKV extends DoFn<Long, KV<Long, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(KV.of(c.element(), "Testval" + c.element()));
    }
  }


  /**
   * Tests writing then reading data in a postgres database.
   * @throws SQLException
   */
  @Test
  public void testWriteAndRead() throws SQLException {

    LOG.info("Started Write");
    runWrite();
    LOG.info("Finished Write, starting Read");

    PCollection<KV<String, Integer>> namesAndIds = pipeline.apply(JdbcIO.<KV<String, Integer>>read()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
        .withQuery(String.format("select name,id from %s;", tableName))
        .withRowMapper(new CreateKVOfNameAndId())
        .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PAssert.thatSingleton(
        namesAndIds.apply("Count All", Count.<KV<String, Integer>>globally()))
        .isEqualTo((long) dataSetExpectedValues.rowCount());

    PCollection<String> consolidatedHashcode = namesAndIds
        .apply(ParDo.of(new SelectNameFn()))
        .apply(Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode).containsInAnyOrder(dataSetExpectedValues.readHash());
    // TODO - break hash into read chunks so we can check on a more granular level?

    pipeline.run().waitUntilFinish();
    LOG.info("Finished Read");
  }

  /**
   * Writes the test dataset to postgres.
   */
  private void runWrite() {
    pipeline.apply(CountingInput.upTo(dataSetExpectedValues.rowCount()))
        .apply(ParDo.of(new ConstructTestDataKV()))
        // TODO - insert shuffle here?
        .apply(JdbcIO.<KV<Long, String>>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withStatement(String.format("insert into %s values(?, ?)", tableName))
            .withPreparedStatementSetter(new PutKeyInColumnOnePutValueInColumnTwo()));

    pipeline.run().waitUntilFinish();
  }
}
