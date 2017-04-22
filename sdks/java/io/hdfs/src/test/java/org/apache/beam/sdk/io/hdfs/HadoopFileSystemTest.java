package org.apache.beam.sdk.io.hdfs;

import com.google.common.collect.Lists;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertFalse;

/**
 * Created by sisk on 4/18/17.
 */
public class HadoopFileSystemTest {
  private FileSystem fileSystem;
  private Configuration configuration;
  private HadoopFileSystem hadoopFileSystem;


  @Before
  public void setup() throws IOException {
    configuration = new Configuration();
    //conf.set("fs.default.name", "hdfs://sisk-single-spark-m.c.sisk-spark-test.internal:8020");
    configuration.set("fs.default.name", "hdfs://10.138.0.5:8020");
    //conf.set("fs.default.name", "hdfs://sisk-single-spark-m:8020");
    configuration.set("hadoop.socks.server", "localhost:1080");
    configuration.set("hadoop.rpc.socket.factory.class.default","org.apache.hadoop.net.SocksSocketFactory");
    fileSystem = FileSystem.get(configuration);

    hadoopFileSystem = HadoopFileSystem.fromConfiguration(configuration);
  }


  /*private void getInterestingFileSystem() throws IOException {

    try() {
      fileSystem.create(new Path("mydir/"));

      Path filePath = new Path("my2nddir/beam-test.txt");
      Path filePath2 = new Path("my2nddir/beam-test2.txt");

      try (FSDataOutputStream out = fileSystem.create(filePath)) {
        out.writeUTF("Hi world! I'm Beaming!");
      }
      try (FSDataOutputStream out = fileSystem.create(filePath2)) {
        out.writeUTF("Hi world! I'm Beaming again!");
      }
    }
    return HadoopFileSystem.fromConfiguration(configuration); // TODO - need a better way of getting a HFS object
  }*/

  /*
  @Test
  public void experimentTest() throws IOException {
    Configuration conf = new Configuration();
    //conf.set("fs.default.name", "hdfs://sisk-single-spark-m.c.sisk-spark-test.internal:8020");
    conf.set("fs.default.name", "hdfs://10.138.0.5:8020");
    //conf.set("fs.default.name", "hdfs://sisk-single-spark-m:8020");
    conf.set("hadoop.socks.server", "localhost:1080");
    conf.set("hadoop.rpc.socket.factory.class.default","org.apache.hadoop.net.SocksSocketFactory");
    try(FileSystem fileSystem = FileSystem.get(conf)) {

      Path filePath = new Path("beam-test.txt");

      try (FSDataOutputStream out = fileSystem.create(filePath)) {
        out.writeUTF("Hi world! I'm Beaming!");
      }

      try(FSDataInputStream in = fileSystem.open(filePath)) {
        String messageIn = in.readUTF();
        System.out.println("read:");
        System.out.print(messageIn);
        throw new IOException(messageIn);
      }

    }
  }
  */

  @Test
  public void deleteDirTest() throws IOException {
    Path delPath = new Path("my2nddir");
    fileSystem.create(delPath);
    hadoopFileSystem.delete(Collections.singletonList(HadoopResourceId.fromPath(delPath)));
    assertFalse(fileSystem.exists(delPath));
  }

  @Test
  public void deleteFileTest() throws IOException {
    hadoopFileSystem.delete(Collections.singletonList(HadoopResourceId.fromPath(new Path("my2nddir/beam-test.txt"))));
  }

  @Test
  public void deleteMultiWithFailure() throws IOException {

  }


}
