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

import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.JDBCExtractionNamespace;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
  private final ConcurrentMap<String, DBI> dbiCache = new ConcurrentHashMap<>();

  @Override
  public Callable<String> getCachePopulator(
      final String id,
      final JDBCExtractionNamespace namespace,
      final String lastVersion,
      final Map<String, String> cache
  )
  {
    final long lastCheck = lastVersion == null ? JodaUtils.MIN_INSTANT : Long.parseLong(lastVersion);
    final Long lastDBUpdate = lastUpdates(id, namespace);
    if (lastDBUpdate != null && lastDBUpdate <= lastCheck) {
      return new Callable<String>()
      {
        @Override
        public String call() throws Exception
        {
          return lastVersion;
        }
      };
    }
    return new Callable<String>()
    {
      @Override
      public String call()
      {
        final long dbQueryStart = System.currentTimeMillis();
        final DBI dbi = ensureDBI(id, namespace);
        final String table = namespace.getTable();
        final String valueColumn = namespace.getValueColumn();
        final String keyColumn = namespace.getKeyColumn();
        final String tsColumn = namespace.getTsColumn();

        LOG.debug("Updating [%s]", id);
        final List<Pair<String, String>> pairs = dbi.withHandle(
            new HandleCallback<List<Pair<String, String>>>()
            {
              @Override
              public List<Pair<String, String>> withHandle(Handle handle) throws Exception
              {
                final String query;
                if(tsColumn == null || namespace.getLastUpdateTime() == null) {
                  query = String.format(
                          "SELECT %s, %s FROM %s",
                          keyColumn,
                          valueColumn,
                          table
                  );

                  LOG.info("JDBC: Performing full updates");
                }else{
                    if(lastDBUpdate > namespace.getLastUpdateTime()){
                      LOG.info("JDBC: Performing partial updates after %s", namespace.getLastUpdateTime());
                      query = String.format(
                              "SELECT %s, %s FROM %s WHERE UNIX_TIMESTAMP(%s) > %s",
                              keyColumn,
                              valueColumn,
                              table,
                              tsColumn,
                              namespace.getLastUpdateTime()
                      );
                    }else{
                      // in theory this code is not reachable
                      // due to previous checks
                      // this code is here only for return integrity
                      LOG.info("Skip loading JDBC: No updates");
                      return new ArrayList<>();
                    }

                }
                namespace.setLastUpdateTime(lastDBUpdate);
                return handle
                    .createQuery(
                        query
                    ).map(
                        new ResultSetMapper<Pair<String, String>>()
                        {

                          @Override
                          public Pair<String, String> map(
                              final int index,
                              final ResultSet r,
                              final StatementContext ctx
                          ) throws SQLException
                          {
                            return new Pair<String, String>(r.getString(keyColumn), r.getString(valueColumn));
                          }
                        }
                    ).list();
              }
            }
        );
        for (Pair<String, String> pair : pairs) {
          cache.put(pair.lhs, pair.rhs);
        }

        LOG.info("Finished loading %d values for namespace[%s]", cache.size(), id);
        if (lastDBUpdate != null) {
          return lastDBUpdate.toString();
        } else {
          return String.format("%d", dbQueryStart);
        }
      }
    };
  }

  private DBI ensureDBI(String id, JDBCExtractionNamespace namespace)
  {
    final String key = id;
    DBI dbi = null;
    if (dbiCache.containsKey(key)) {
      dbi = dbiCache.get(key);
    }
    if (dbi == null) {
      final DBI newDbi = new DBI(
          namespace.getConnectorConfig().getConnectURI(),
          namespace.getConnectorConfig().getUser(),
          namespace.getConnectorConfig().getPassword()
      );
      dbiCache.putIfAbsent(key, newDbi);
      dbi = dbiCache.get(key);
    }
    return dbi;
  }

  private Long lastUpdates(String id, JDBCExtractionNamespace namespace)
  {
    final DBI dbi = ensureDBI(id, namespace);
    final String table = namespace.getTable();
    final String tsColumn = namespace.getTsColumn();
    final String LATEST = "latest";

    if (tsColumn == null) {
      return null;
    }
    final Long update = dbi.withHandle(
        new HandleCallback<Long>()
        {

          @Override
          public Long withHandle(Handle handle) throws Exception
          {
            final String query = String.format(
                "SELECT MAX(UNIX_TIMESTAMP(%s)) AS %s FROM %s",
                tsColumn, LATEST, table
            );
            return handle
                .createQuery(query)
                .map(
                        new ResultSetMapper<Long>() {

                          @Override
                          public Long map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
                            return resultSet.getLong(LATEST);
                          }
                        }
                ).first();
          }
        }
    );
    return update;
  }
}
