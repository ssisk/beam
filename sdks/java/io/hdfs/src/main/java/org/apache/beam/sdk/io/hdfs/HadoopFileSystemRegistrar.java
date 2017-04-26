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
package org.apache.beam.sdk.io.hdfs;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * {@link AutoService} registrar for the {@link HadoopFileSystem}.
 */
@AutoService(FileSystemRegistrar.class)
public class HadoopFileSystemRegistrar implements FileSystemRegistrar {

  @Override
  public FileSystem fromOptions(@Nonnull PipelineOptions options) throws IOException {
    return HadoopFileSystem.fromConfiguration(constructConfiguration(options));
  }

  @VisibleForTesting
  private Configuration constructConfiguration(@Nonnull PipelineOptions options) {
    Configuration configuration = new Configuration();
    HadoopFileSystemOptions hfsOptions = options.as(HadoopFileSystemOptions.class);
    Map<String, String> hfsBag = hfsOptions.getHfsConfiguration();
    for(Map.Entry<String,String> entry : hfsBag.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }
    return configuration;
  }

  @Override
  public String getScheme() {
    return "hdfs";
  }
}
