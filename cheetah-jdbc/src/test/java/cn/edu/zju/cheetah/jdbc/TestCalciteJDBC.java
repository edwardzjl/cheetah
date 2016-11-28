package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.ConversionUtil;
import org.junit.Test;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahSchemaFactory;

public class TestCalciteJDBC extends TestCase {

  private static final String BROKER = "http://10.214.208.42:8082";
  private static final String COORDINATOR = "http://10.214.208.42:8081";

  @Test
  public void test() throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");

    System.setProperty("saffron.default.charset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    System.setProperty("saffron.default.nationalcharset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    System.setProperty("saffron.default.collation.name",
        ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");
    
    Properties info = new Properties();
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    System.out.println(calciteConnection);

    Map<String, Object> operand = new HashMap<>();
    operand.put("url", BROKER);
    operand.put("coordinatorUrl", COORDINATOR);

    CheetahSchemaFactory schemaFactory = new CheetahSchemaFactory();
    Schema schema = schemaFactory.create(calciteConnection.getRootSchema(), "default", operand);
    calciteConnection.getRootSchema().add("default", schema);
    calciteConnection.setSchema("default");
 
    DatabaseMetaData metaData = calciteConnection.getMetaData();
    try (ResultSet rs = metaData.getTables(null, null, null, null)) {
      while (rs.next()) {
        System.out.println(rs.getString("table_name") + " " + rs.getString("table_type"));
      }
    }
    
    try (ResultSet rs = metaData.getColumns(null, null, "wikiticker", null)) {
      printResultSet(rs);
    }
    
    Statement stmt = calciteConnection.createStatement();
    String sql = "SELECT \"cityName\" FROM \"wikiticker\" LIMIT 20";
    try (ResultSet rs = stmt.executeQuery(sql)) {
      printResultSet(rs);
    }
  }

}
