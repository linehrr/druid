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
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import org.jetbrains.annotations.NotNull;
import org.skife.jdbi.v2.DBI;

import java.lang.ref.WeakReference;
import java.sql.*;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 *
 */
public class OffHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger log = new Logger(OffHeapNamespaceExtractionCacheManager.class);
  private Striped<Lock> nsLocks = Striped.lazyWeakLock(1024); // Needed to make sure delete() doesn't do weird things

  private final ConcurrentMap<String, ConcurrentMap<String, String>> swapMaps = new ConcurrentHashMap<>();

  @Inject
  public OffHeapNamespaceExtractionCacheManager(
      Lifecycle lifecycle,
      ServiceEmitter emitter,
      final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> namespaceFunctionFactoryMap
  )
  {
    super(lifecycle, emitter, namespaceFunctionFactoryMap);

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
              // NOOP
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
    swapMaps.put(namespaceKey, swapMaps.get(cacheKey));
    swapMaps.remove(cacheKey);
    return true;
  }

  @Override
  public ConcurrentMap<String, String> getCacheMap(String namespaceKey)
  {
    if(swapMaps.containsKey(namespaceKey)){
      ConcurrentMap<String, String> map = swapMaps.get(namespaceKey);
      log.info("Returned existing JDBC callback map for table %s, cached: %s", namespaceKey,
              map.size());
      return map;
    }else {
      JDBCcallbackMap map = new JDBCcallbackMap();
      swapMaps.put(namespaceKey,  map);
      log.info("Returned a new JDBC callback instance for %s", namespaceKey);
      return map;
    }
  }

  public class JDBCcallbackMap implements ConcurrentMap<String, String> {

    String table;
    String keyCol;
    String valueCol;
    DBI dbi;
    Connection conn;
    private ConcurrentMap<String, String> cache = new ConcurrentHashMap<>();

    public JDBCcallbackMap setTable(String table) {
      this.table = table;
      return this;
    }
    public JDBCcallbackMap setKeyCol(String keyCol) {
      this.keyCol = keyCol;
      return this;
    }
    public JDBCcallbackMap setValueCol(String valueCol) {
      this.valueCol = valueCol;
      return this;
    }
    public JDBCcallbackMap setDBI(DBI dbi) {
      this.dbi = dbi;
      this.conn = dbi.open().getConnection();
      return this;
    }

    @Override
    public int size() {
      return cache.size();
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean containsKey(Object o) {
      return true;
    }

    @Override
    public boolean containsValue(Object o) {
      return false;
    }

    @Override
    public String get(Object o) {
      if(cache.containsKey(o)){
       return cache.get(o);
      }else {
        String query = String.format(
                "SELECT %s FROM %s WHERE %s = ?",
                valueCol,
                table,
                keyCol
        );
        try {
          PreparedStatement st = conn.prepareStatement(query);
          st.setString(1, (String) o);

          ResultSet res = st.executeQuery();

          if (res.next()) {
            cache.put((String) o, res.getString(valueCol));
            return res.getString(valueCol);
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }
        return null;
      }
    }

    @Override
    public String put(String s, String s2) {
      return null;
    }

    @Override
    public String remove(Object o) {
      return null;
    }

    @Override
    public void putAll(@NotNull Map<? extends String, ? extends String> map) {

    }

    @Override
    public void clear() {

    }

    @NotNull
    @Override
    public Set<String> keySet() {
      return null;
    }

    @NotNull
    @Override
    public Collection<String> values() {
      return null;
    }

    @NotNull
    @Override
    public Set<Entry<String, String>> entrySet() {
      return null;
    }

    @Override
    public String putIfAbsent(@NotNull String s, String s2) {
      return null;
    }

    @Override
    public boolean remove(@NotNull Object o, Object o1) {
      return false;
    }

    @Override
    public boolean replace(@NotNull String s, @NotNull String s2, @NotNull String v1) {
      return false;
    }

    @Override
    public String replace(@NotNull String s, @NotNull String s2) {
      return null;
    }
  }

  @Override
  protected void monitor(ServiceEmitter serviceEmitter)
  {
//    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/diskSize", tmpFile.length()));
  }
}
