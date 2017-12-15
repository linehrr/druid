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

import com.metamx.common.logger.Logger;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.JDBCExtractionNamespace;
import io.druid.server.lookup.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import org.apache.commons.dbcp2.BasicDataSource;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class JDBCExtractionNamespaceCacheFactory
    implements ExtractionNamespaceCacheFactory<JDBCExtractionNamespace>
{
  private static final Logger LOG = new Logger(JDBCExtractionNamespaceCacheFactory.class);
  private final ConcurrentMap<String, BasicDataSource> connPools = new ConcurrentHashMap<>();

  @Override
  public Callable<String> getCachePopulator(
      final String id,
      final JDBCExtractionNamespace namespace,
      final String lastVersion,
      final Map<String, String> cache
  )
  {
    return new Callable<String>()
    {
      @Override
      public String call() throws Exception
      {
        ((OffHeapNamespaceExtractionCacheManager.JDBCcallbackMap)cache)
                .setKeyCol(namespace.getKeyColumn())
                .setTable(namespace.getTable())
                .setValueCol(namespace.getValueColumn())
                .setDBI(getConnectionPool(namespace));

        LOG.info("Setup %s-%s-%s ready!", namespace.getTable(), namespace.getKeyColumn(), namespace.getValueColumn());
        return String.format("%s-%s-%s", namespace.getTable(), namespace.getKeyColumn(), namespace.getValueColumn());
      }
    };
  }

  private BasicDataSource getConnectionPool(JDBCExtractionNamespace namespace)
  {
    String url = namespace.getConnectorConfig().getConnectURI();
    String poolKey = String.format("%s-%s-%s",
            url,
            namespace.getConnectorConfig().getUser(),
            namespace.getConnectorConfig().getPassword()
            );

    BasicDataSource pool;
    if (connPools.containsKey(poolKey)) {
      LOG.info("Return existing pool for url: %s", url);
      pool = connPools.get(poolKey);
    }else{
      LOG.info("Creating new connection pool for url: %s", url);

      pool = new BasicDataSource();
      pool.setDriverClassName("com.mysql.jdbc.Driver");
      pool.setUrl(url);
      pool.setUsername(namespace.getConnectorConfig().getUser());
      pool.setPassword(namespace.getConnectorConfig().getPassword());
      pool.setMaxTotal(4);
      pool.setMinIdle(0);

      connPools.putIfAbsent(poolKey, pool);
    }
    return pool;
  }

}
