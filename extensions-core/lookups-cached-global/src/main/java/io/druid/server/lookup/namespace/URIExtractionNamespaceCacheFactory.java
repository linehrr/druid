/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.lookup.namespace;

import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.IAE;
import com.metamx.common.RetryUtils;
import com.metamx.common.logger.Logger;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.data.input.MapPopulator;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.URIExtractionNamespace;
import io.druid.segment.loading.URIDataPuller;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 *
 */
public class URIExtractionNamespaceCacheFactory implements ExtractionNamespaceCacheFactory<URIExtractionNamespace>
{
  private static final int DEFAULT_NUM_RETRIES = 3;
  private static final Logger log = new Logger(URIExtractionNamespaceCacheFactory.class);
  private final Map<String, SearchableVersionedDataFinder> pullers;

  @Inject
  public URIExtractionNamespaceCacheFactory(
      Map<String, SearchableVersionedDataFinder> pullers
  )
  {
    this.pullers = pullers;
  }

  @Override
  public Callable<String> getCachePopulator(
      final String id,
      final URIExtractionNamespace extractionNamespace,
      @Nullable final String lastVersion,
      final Map<String, String> cache
  )
  {
      return new Callable<String>() {
        @Override
        public String call() throws Exception {
          return "DUMMY";
        }
      };
  }
}
