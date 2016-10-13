package cn.edu.zju.cheetah.jdbc;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link TableFactory} for Cheetah.
 *
 * <p>A table corresponds to what Cheetah calls a "data source".
 */
public class CheetahTableFactory implements TableFactory {
  @SuppressWarnings("unused")
  public static final CheetahTableFactory INSTANCE = new CheetahTableFactory();

  private CheetahTableFactory() {}

  public Table create(SchemaPlus schema, String name, Map operand,
      RelDataType rowType) {
    final CheetahSchema cheetahSchema = schema.unwrap(CheetahSchema.class);
    // If "dataSource" operand is present it overrides the table name.
    final String dataSource = (String) operand.get("dataSource");
    final Set<String> metricNameBuilder = new LinkedHashSet<>();
    final Map<String, SqlTypeName> fieldBuilder = new LinkedHashMap<>();
    final String timestampColumnName;
    if (operand.get("timestampColumn") != null) {
      timestampColumnName = (String) operand.get("timestampColumn");
    } else {
      timestampColumnName = CheetahTable.DEFAULT_TIMESTAMP_COLUMN;
    }
    fieldBuilder.put(timestampColumnName, SqlTypeName.TIMESTAMP);
    final Object dimensionsRaw = operand.get("dimensions");
    if (dimensionsRaw instanceof List) {
      //noinspection unchecked
      final List<String> dimensions = (List<String>) dimensionsRaw;
      for (String dimension : dimensions) {
        fieldBuilder.put(dimension, SqlTypeName.VARCHAR);
      }
    }
    final Object metricsRaw = operand.get("metrics");
    if (metricsRaw instanceof List) {
      final List metrics = (List) metricsRaw;
      for (Object metric : metrics) {
        final SqlTypeName sqlTypeName;
        final String metricName;
        if (metric instanceof Map) {
          Map map2 = (Map) metric;
          if (!(map2.get("name") instanceof String)) {
            throw new IllegalArgumentException("metric must have name");
          }
          metricName = (String) map2.get("name");

          final Object type = map2.get("type");
          if ("long".equals(type)) {
            sqlTypeName = SqlTypeName.BIGINT;
          } else if ("double".equals(type)) {
            sqlTypeName = SqlTypeName.DOUBLE;
          } else {
            sqlTypeName = SqlTypeName.BIGINT;
          }
        } else {
          metricName = (String) metric;
          sqlTypeName = SqlTypeName.BIGINT;
        }
        fieldBuilder.put(metricName, sqlTypeName);
        metricNameBuilder.add(metricName);
      }
    }
    final String dataSourceName = Util.first(dataSource, name);
    CheetahConnectionImpl c;
    if (dimensionsRaw == null || metricsRaw == null) {
      c = new CheetahConnectionImpl(cheetahSchema.url, cheetahSchema.url.replace(":8082", ":8081"));
    } else {
      c = null;
    }
    final Object intervalString = operand.get("interval");
    final List<Interval> intervals;
    if (intervalString instanceof String) {
      intervals = ImmutableList.of(
          new Interval(intervalString, ISOChronology.getInstanceUTC()));
    } else {
      intervals = null;
    }
    return CheetahTable.create(cheetahSchema, dataSourceName, intervals,
        fieldBuilder, metricNameBuilder, timestampColumnName, c);
  }

}

// End CheetahTableFactory.java
