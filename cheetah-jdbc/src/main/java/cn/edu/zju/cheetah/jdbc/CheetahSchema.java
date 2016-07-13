/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * @author JIANG
 *
 */
public class CheetahSchema extends AbstractSchema {
  final String url;
  final String coordinatorUrl;
  private final boolean discoverTables;

  /**
   * Creates a Druid schema.
   *
   * @param url URL of query REST service, e.g. "http://localhost:8082"
   * @param coordinatorUrl URL of coordinator REST service,
   *                       e.g. "http://localhost:8081"
   * @param discoverTables If true, ask Druid what tables exist;
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
    final CheetahCalciteConnection connection =
        new CheetahCalciteConnection(url, coordinatorUrl);
    return Maps.asMap(ImmutableSet.copyOf(connection.tableNames()),
        CacheBuilder.<String, Table>newBuilder()
            .build(new CacheLoader<String, Table>() {
              public Table load(@Nonnull String tableName) throws Exception {
                final Map<String, SqlTypeName> fieldMap = new LinkedHashMap<>();
                final Set<String> metricNameSet = new LinkedHashSet<>();
                connection.metadata(tableName, null, fieldMap, metricNameSet);
                return CheetahTable.create(CheetahSchema.this, tableName,
                    null, fieldMap, metricNameSet, null, connection);
              }
            }));
  }
}
