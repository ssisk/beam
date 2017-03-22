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

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;

import org.apache.beam.sdk.io.common.DataSetExpectedValues;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manipulates test data used by the {@link org.apache.beam.sdk.io.jdbc.JdbcIO} tests.
 *
 * <p>This is independent from the tests so that for read tests it can be run separately after data
 * store creation rather than every time (which can be more fragile.)
 */
public class JdbcTestDataSet {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTestDataSet.class);
  public static final String[] SCIENTISTS = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie",
      "Faraday", "McClintock", "Herschel", "Hopper", "Lovelace"};
  public static final Map<IOTestPipelineOptions.DataSetSize, DataSetExpectedValues>
      DATA_SET_EXPECTATION_MAP = ImmutableMap.of(
      // TODO - still need to add support for the writeHash
      IOTestPipelineOptions.DataSetSize.SMALL, DataSetExpectedValues.create(1000,
          "TODOasdfsmallwritehash", "79735d27304f0b55e61fcdfd443e910a6a8d8ee4"),
      // TODO - LARGE is intended to be a much larger number, but for now keeping it small.
      IOTestPipelineOptions.DataSetSize.LARGE, DataSetExpectedValues.create(10000,
          "TODOasdflargewritehash", "b3c1424b059f93e9946d3cb2c8111101351f0f1e")
  );


  private static DataSetExpectedValues expectedDataSetValues;
  /**
   * Use this to create the read tables before IT read tests.
   *
   * <p>To invoke this class, you can use this command line:
   * (run from the jdbc root directory)
   * mvn test-compile exec:java -Dexec.mainClass=org.apache.beam.sdk.io.jdbc.JdbcTestDataSet \
   *   -Dexec.args="--postgresServerName=127.0.0.1 --postgresUsername=postgres \
   *   --postgresDatabaseName=myfancydb \
   *   --postgresPassword=yourpassword --postgresSsl=false" \
   *   -Dexec.classpathScope=test
   * @param args Please pass options from IOTestPipelineOptions used for connection to postgres as
   * shown above.
   */
  public static void main(String[] args) throws SQLException {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(IOTestPipelineOptions.class);

    createReadDataTableAndAddInitialData(getDataSource(options),
        JdbcTestDataSet.DATA_SET_EXPECTATION_MAP.get(options.getDataSetSize()));
  }

  public static PGSimpleDataSource getDataSource(IOTestPipelineOptions options)
      throws SQLException {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();

    // Tests must receive parameters for connections from PipelineOptions
    // Parameters should be generic to all tests that use a particular datasource, not
    // the particular test.
    dataSource.setDatabaseName(options.getPostgresDatabaseName());
    dataSource.setServerName(options.getPostgresServerName());
    dataSource.setPortNumber(options.getPostgresPort());
    dataSource.setUser(options.getPostgresUsername());
    dataSource.setPassword(options.getPostgresPassword());
    dataSource.setSsl(options.getPostgresSsl());

    return dataSource;
  }

  public static final String READ_TABLE_NAME = "BEAM_TEST_READ";

  public static void createReadDataTableAndAddInitialData(
      DataSource dataSource, DataSetExpectedValues dataSetExpectedValues) throws SQLException {
    createDataTable(dataSource, READ_TABLE_NAME);
    addInitialData(dataSource, READ_TABLE_NAME, dataSetExpectedValues);
  }

  public static String createWriteDataTableAndAddInitialData(
      DataSource dataSource, DataSetExpectedValues dataSetExpectedValues) throws SQLException {
    String tableName = getWriteTableName();
    createDataTable(dataSource, tableName);
    addInitialData(dataSource, tableName, dataSetExpectedValues);
    return tableName;
  }

  public static String getWriteTableName() {
    return "BEAMTEST" + org.joda.time.Instant.now().getMillis();
  }

  private static void addInitialData(
      DataSource dataSource, String tableName, DataSetExpectedValues dataSetExpectedValues)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try (PreparedStatement preparedStatement =
               connection.prepareStatement(
                   String.format("insert into %s values (?,?)", tableName))) {
        for (int i = 0; i < dataSetExpectedValues.rowCount(); i++) {
          int index = i % SCIENTISTS.length;
          preparedStatement.clearParameters();
          preparedStatement.setInt(1, i);
          preparedStatement.setString(2, SCIENTISTS[index]);
          preparedStatement.executeUpdate();
        }
      }
      connection.commit();
    }
  }

  public static void createDataTable(
      DataSource dataSource, String tableName)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format("create table %s (id INT, name VARCHAR(500))", tableName));
      }
    }

    LOG.info("Created table {}", tableName);
  }

  public static void cleanUpDataTable(DataSource dataSource, String tableName)
      throws SQLException {
    if (tableName != null) {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("drop table %s", tableName));
      }
    }
  }

}
