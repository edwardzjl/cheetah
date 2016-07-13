/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * @author JIANG
 *
 */
public class CheetahSchemaFactory implements SchemaFactory {

  public static final String DEFAULT_URL = "http://localhost:8082";

  public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    final Map<String, Object> map = operand;
    final String url = map.get("url") instanceof String
        ? (String) map.get("url")
        : DEFAULT_URL;
    final String coordinatorUrl = map.get("coordinatorUrl") instanceof String
        ? (String) map.get("coordinatorUrl")
        : url.replace(":8082", ":8081");
    // "tables" is a hidden attribute, copied in from the enclosing custom
    // schema
    final boolean containsTables = map.get("tables") instanceof List
        && ((List<?>) map.get("tables")).size() > 0;
    return new CheetahSchema(url, coordinatorUrl, !containsTables);
  }

}
