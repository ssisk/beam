package org.apache.beam.sdk.io.common;

import com.google.auto.value.AutoValue;

/**
 * Helper class for IO ITs that want to define expected counts & hashes of rows.
 */
@AutoValue
public abstract class DataSetExpectedValues {
  public static DataSetExpectedValues create(int rowCount, String writeHash, String readHash) {
    return new AutoValue_DataSetExpectedValues(rowCount, writeHash, readHash);
  }

  public abstract int rowCount();
  public abstract String writeHash(); // TODO - unused for now
  public abstract String readHash();

  public static final String SMALL = "small";
  public static final String LARGE = "large";

}
