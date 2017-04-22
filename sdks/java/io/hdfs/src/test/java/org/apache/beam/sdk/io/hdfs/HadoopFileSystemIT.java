package org.apache.beam.sdk.io.hdfs;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;


public class HadoopFileSystemIT {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  private static MiniDFSCluster cluster;


  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).build();

    cluster.waitActive();
  }

  @Test
  public void simpleRead() throws IOException {
    /*
     user runs pipeline:
     mvn java:exec -DpipelineOptions=[--hfsConfiguration={'fs.default.name': 'hdfs://10.138.0.5:8020'}]
     */

    FileSystem fileSystem = cluster.getFileSystem();
    Path testFile = new Path("sisk-test-file.txt");

    assertFalse(fileSystem.exists(testFile));

    try (FSDataOutputStream out = fileSystem.create(testFile)) {
      out.writeUTF("Hi world! I'm Beaming!");
    }

    // Note that you still have to specify the server - is that correct?
    /*
    PCollection<GenericRecord> myStrings =
        pipeline.apply(
            AvroIO.Read.from("hdfs://127.0.0.1:" + cluster.getNameNodePort() + "/sisk-test-file.txt")
                       .withSchema("{\"namespace\": \"example.avro\",\n" +
                           " \"type\": \"record\",\n" +
                           " \"name\": \"User\",\n" +
                           " \"fields\": [\n" +
                           "     {\"name\": \"name\", \"type\": \"string\"}\n" +
                           " ]\n" +
                           "}"));
                           */
    PCollection<String> myStrings =
        pipeline.apply(TextIO.Read.from("hdfs://127.0.0.1:" + cluster.getNameNodePort() + "/sisk-test-file.txt"));

    //PAssert.that(myStrings).containsInAnyOrder("hello", "world");


    // if relative hdfs paths should work:
  /*  PCollection<String> myStrings2 =
        pipeline.apply(TextIO.Read.from("hdfs://10.138.0.5:8020/beam-test.txt"));

    PAssert.that(myStrings2).containsInAnyOrder("hello", "world");
*/
    pipeline.run();
  }

  @AfterClass
  public static void teardown() {
    cluster.shutdown();
  }
}
