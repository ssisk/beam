package org.apache.beam.sdk.io.hdfs;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by sisk on 4/20/17.
 */
public class HadoopResourceIdTest {

  @Test
  public void handlesWindowsDirectories() {

  }

  @Test
  public void handlesRelativePathsAddedToDir() {

  }

  // TODO - lots of tests involving / at the end of various parameters :)
  // TODO - what happens if we pass in a null path (which is the root) to a resource id?

  @Test
  public void testScheme() {
    assertEquals("hdfs", HadoopResourceId.fromPath(new Path("hdfs://myhost:8020/mydir/file.txt")).getScheme());
    // TODO - not sure if this behavior is correct, but documenting that this is what is happening now.
    assertEquals(null, HadoopResourceId.fromPath(new Path("/tmp/file.txt")).getScheme());
  }
}
