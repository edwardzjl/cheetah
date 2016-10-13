package cn.edu.zju.cheetah.jdbc;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Compatible;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Schema mapped onto a Cheetah instance.
 */
public class CheetahSchema extends AbstractSchema {
  final String url;
  final String coordinatorUrl;
  private final boolean discoverTables;

  /**
   * Creates a Cheetah schema.
   *
   * @param url URL of query REST service, e.g. "http://localhost:8082"
   * @param coordinatorUrl URL of coordinator REST service,
   *                       e.g. "http://localhost:8081"
   * @param discoverTables If true, ask Cheetah what tables exist;
   *                       if false, only create tables explicitly in the model
   */
  public CheetahSchema(String url, String coordinatorUrl,
                       boolean discoverTables) {
    this.url = Preconditions.checkNotNull(url);
    this.coordinatorUrl = Preconditions.checkNotNull(coordinatorUrl);
    this.discoverTables = discoverTables;
  }

  @Override protected Map<String, Table> getTableMap() {
    if (!discoverTables) {
      return ImmutableMap.of();
    }
    final CheetahConnectionImpl connection =
        new CheetahConnectionImpl(url, coordinatorUrl);
    return Compatible.INSTANCE.asMap(
        ImmutableSet.copyOf(connection.tableNames()),
        CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Table>() {
              public Table load(@Nonnull String tableName) throws Exception {
                final Map<String, SqlTypeName> fieldMap = new LinkedHashMap<>();
                final Set<String> metricNameSet = new LinkedHashSet<>();
                connection.metadata(tableName, CheetahTable.DEFAULT_TIMESTAMP_COLUMN,
                    null, fieldMap, metricNameSet);
                return CheetahTable.create(CheetahSchema.this, tableName, null,
                    fieldMap, metricNameSet, CheetahTable.DEFAULT_TIMESTAMP_COLUMN, connection);
              }
            }));
  }
}

// End CheetahSchema.java
