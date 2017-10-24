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

package io.druid.server.lookup.namespace.cache;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 *
 */
public class OffHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger log = new Logger(OffHeapNamespaceExtractionCacheManager.class);
  private final DB onDiskDB;
  private final DB inMemDB;
  private Striped<Lock> nsLocks = Striped.lazyWeakLock(1024); // Needed to make sure delete() doesn't do weird things
  private final File tmpFile;

  private final String mapDBKey = "MAPDB";

  @Inject
  public OffHeapNamespaceExtractionCacheManager(
      Lifecycle lifecycle,
      ServiceEmitter emitter,
      final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> namespaceFunctionFactoryMap
  )
  {
    super(lifecycle, emitter, namespaceFunctionFactoryMap);
    try {
      tmpFile = File.createTempFile("druidMapDB", getClass().getCanonicalName());
      log.info("Using file [%s] for mapDB off heap namespace cache", tmpFile.getAbsolutePath());
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    onDiskDB = DBMaker
        .fileDB(tmpFile)
        .fileDeleteAfterClose()
        .closeOnJvmShutdown()
        .make();

    inMemDB = DBMaker
            .memoryDB()
            .make();

    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              // NOOP
            }

            @Override
            public synchronized void stop()
            {
              if (!onDiskDB.isClosed()) {
                onDiskDB.close();
                if (!tmpFile.delete()) {
                  log.warn("Unable to delete file at [%s]", tmpFile.getAbsolutePath());
                }
              }

              if(!inMemDB.isClosed()) {
                inMemDB.close();
              }
            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected boolean swapAndClearCache(String namespaceKey, String cacheKey)
  {
      return true;
  }

  @Override
  public ConcurrentMap<String, String> getCacheMap(String namespaceKey)
  {
    final Lock lock = nsLocks.get(namespaceKey);
    lock.lock();
    try {

      HTreeMap<String, String> onDiskMap = onDiskDB
              .hashMap(mapDBKey)
              .keySerializer(Serializer.STRING)   // specify KV serde for performance
              .valueSerializer(Serializer.STRING)
              .counterEnable()
              .createOrOpen();

      ConcurrentMap<String, String> retMap = inMemDB
              .hashMap(mapDBKey)
              .keySerializer(Serializer.STRING)   // specify KV serde for performance
              .valueSerializer(Serializer.STRING)
              .counterEnable()  // enable counter for size() call
              .expireAfterGet(1L, TimeUnit.DAYS)
              .expireOverflow(onDiskMap)
              .createOrOpen();

      log.info("OffHeapMap loaded(%s): inMem: %s, onDisk: %s", namespaceKey, retMap.size(), onDiskMap.size());
      return retMap;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  protected void monitor(ServiceEmitter serviceEmitter)
  {
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/diskSize", tmpFile.length()));
  }
}
