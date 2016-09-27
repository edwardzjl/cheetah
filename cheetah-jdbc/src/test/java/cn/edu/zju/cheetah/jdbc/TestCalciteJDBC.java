/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    System.out.println(calciteConnection);

    CheetahSchemaFactory schemaFactory = new CheetahSchemaFactory();
    Map<String, Object> operand = new HashMap<>();
    String broker = "http://10.214.208.59:8082";
    String coordinator = "http://10.214.208.59:8081";
    operand.put("url", broker);
    operand.put("coordinatorUrl", coordinator);
    Schema schema = schemaFactory.create(calciteConnection.getRootSchema(), 
        "default", operand);
    calciteConnection.getRootSchema().add("default", schema);
    calciteConnection.setSchema("default");
 
    DatabaseMetaData metaData = calciteConnection.getMetaData();
    try (ResultSet rs = metaData.getTables(null, null, null, null)) {
      while (rs.next()) {
        System.out.println(rs.getString("table_name") + " " + rs.getString("table_type"));
      }
    }
    
    try (ResultSet rs = metaData.getColumns(null, null, "wikiticker", null)) {
      while (rs.next()) {
        for (int i = 1; i <= 6; i++) {
          System.out.print(rs.getString(i) + " | ");
        }
        System.out.println();
      }
    }
    
//    Statement stmt = calciteConnection.createStatement();
//    String sql = "SELECT * FROM \"wikiticker\" limit 20";
//    try (ResultSet rs = stmt.executeQuery(sql)) {
//      while (rs.next()) {
//        for (int i = 1; i <= 11; i++) {
//          System.out.print(rs.getString(i) + " | ");
//        }
//        System.out.println();
//      }
//    }
  }

}
