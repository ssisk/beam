package org.apache.beam.sdk.io.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by sisk on 4/18/17.
 */
public class HadoopFileSystemTest {
  @Test
  public void experimentTest() throws IOException {
    // note to self: may need to configure java to connect through socks proxy -
    // https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html
    // http://stackoverflow.com/questions/34267443/maven-to-use-socks-proxy-for-specific-repo
    System.setProperty("socksProxyHost", "127.0.0.1");
    System.setProperty("socksProxyPort", "1080");
    System.clearProperty("http.proxyHost");
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://sisk-hdfs-experiments-m:8020");
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

  //  System.clearProperty("socksProxyHost");
  //  System.clearProperty("socksProxyPort");

  }
}
