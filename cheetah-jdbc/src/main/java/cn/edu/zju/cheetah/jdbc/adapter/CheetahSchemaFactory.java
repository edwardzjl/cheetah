package cn.edu.zju.cheetah.jdbc.adapter;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

/**
 * Schema factory that creates Cheetah schemas.
 *
 * <table>
 *   <caption>Cheetah schema operands</caption>
 *   <tr>
 *     <th>Operand</th>
 *     <th>Description</th>
 *     <th>Required</th>
 *   </tr>
 *   <tr>
 *     <td>url</td>
 *     <td>URL of Cheetah's query node.
 *     The default is "http://localhost:8082".</td>
 *     <td>No</td>
 *   </tr>
 *   <tr>
 *     <td>coordinatorUrl</td>
 *     <td>URL of Cheetah's coordinator node.
 *     The default is <code>url</code>, replacing "8082" with "8081",
 *     for example "http://localhost:8081".</td>
 *     <td>No</td>
 *   </tr>
 * </table>
 */
public class CheetahSchemaFactory implements SchemaFactory {
  /** Default Cheetah URL. */
  public static final String DEFAULT_URL = "http://localhost:8082";

  public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    final String url = operand.get("url") instanceof String
        ? (String) operand.get("url")
        : DEFAULT_URL;
    final String coordinatorUrl = operand.get("coordinatorUrl") instanceof String
        ? (String) operand.get("coordinatorUrl")
        : url.replace(":8082", ":8081");
    // "tables" is a hidden attribute, copied in from the enclosing custom
    // schema
    final boolean containsTables = operand.get("tables") instanceof List
        && ((List) operand.get("tables")).size() > 0;
    return new CheetahSchema(url, coordinatorUrl, !containsTables);
  }
}

// End CheetahSchemaFactory.java
