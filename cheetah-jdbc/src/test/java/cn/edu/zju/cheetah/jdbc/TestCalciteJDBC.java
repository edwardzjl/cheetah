/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.adapter.druid.DruidSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;

import junit.framework.TestCase;

/**
 * @author JIANG
 *
 */
public class TestCalciteJDBC extends TestCase {

  public void testCalciteJDBC() throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
//    info.put("model",
//        "inline:"
//            + "{\n"
//            + "  version: '1.0',\n"
//            + "   schemas: [\n"
//            + "     {\n"
//            + "       type: 'custom',\n"
//            + "       name: 'bad',\n"
//            + "       factory: 'org.apache.calcite.adapter.druid.DruidSchemaFactory',\n"
//            + "       operand: {\n"
//            + "         url: 'http://10.214.208.59:8082',\n"
//            + "         coordinatorUrl: 'http://10.214.208.59:8081'"
//            + "       },\n"
//            + "   tables: [ {\n"
//            + "       name: 'metrics',\n"
//            + "       factory: 'org.apache.calcite.adapter.druid.DruidTableFactory',\n"
//            + "       operand: { \n"
//            + "          dataSource: 'metrics',\n"
//            + "          interval: '1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z',\n"
//            + "          timestampColumn: '__time'\n"
//            + "        }\n"
//            + "       }\n"
//            + "    ]\n"
//            + "   }\n"
//            + "  ]\n"
//            + "}");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    System.out.println(calciteConnection);

    DruidSchemaFactory schemaFactory = new DruidSchemaFactory();
    Map<String, Object> operand = new HashMap<>();
    String broker = "http://10.214.208.59:8082";
    String coordinator = "http://10.214.208.59:8081";
    operand.put("url", broker);
    operand.put("coordinatorUrl", coordinator);
    Schema schema = schemaFactory.create(calciteConnection.getRootSchema(), 
        "default", operand);
    calciteConnection.getRootSchema().add("default", schema);
    System.out.println(schema.getTable("metrics"));
    calciteConnection.getRootSchema().add("metrics", schema.getTable("metrics"));

    DatabaseMetaData metaData = calciteConnection.getMetaData();
    try (ResultSet rs = metaData.getTables(null, null, null, null)) {
      while (rs.next()) {
        System.out.println(rs.getString("table_name") + " " + rs.getString("table_type"));
      }
    }
    
    try (ResultSet rs = metaData.getColumns(null, null, "metrics", null)) {
      while (rs.next()) {
        for (int i = 1; i <= 6; i++) {
          System.out.print(rs.getString(i) + " | ");
        }
        System.out.println();
      }
    }
    
    Statement stmt = calciteConnection.createStatement();
    String sql = "SELECT * FROM metrics";
    try (ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        for (int i = 1; i <= 11; i++) {
          System.out.print(rs.getString(i) + " | ");
        }
        System.out.println();
      }
    }
  }

}
