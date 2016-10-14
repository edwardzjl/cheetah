package cn.edu.zju.cheetah.jdbc;

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestCheetahDriver extends TestCase {

  private final String SQL = "SELECT COUNT(*) "
      + "FROM \"uniq_submit_order\" "
      + "WHERE \"fourth_category_name\" = 'DHA' AND \"clienttype\" = 'WEB'";

  @Test
  public void testConnectViaUrl() throws SQLException {
    Connection dbConn = connectViaUrl();
    dbConn.close();
  }

  @Test
  public void testConnectViaProperties() throws SQLException {
    Connection dbConn = connectViaProperties();
    dbConn.close();
  }

  @Test
  public void testDatabaseMetaData() throws SQLException {
    Connection dbConn = connectViaProperties();

    DatabaseMetaData metaData = dbConn.getMetaData();
    assertNotNull(metaData);

    try (ResultSet rs = metaData.getColumns(null, null, "wikiticker", "cityName")) {
      System.out.println("getColumns: ");
      printResultSet(rs);
      rs.close();
    }

    try (ResultSet rs = metaData.getTables(null, null, "wikiticker", null)) {
      System.out.println("getTables: ");
      printResultSet(rs);
      rs.close();
    }

    dbConn.close();
  }

  @Test
  public void testStatement() throws SQLException {
    Connection dbConn = connectViaProperties();
    Statement stmt = dbConn.createStatement();

    try (ResultSet rs = stmt.executeQuery(SQL)) {
      rs.close();
    }

    stmt.close();
    dbConn.close();
  }

  @Test
  public void testResultSetMetaData() throws SQLException {
    Connection dbConn = connectViaProperties();
    Statement stmt = dbConn.createStatement();

    try (ResultSet rs = stmt.executeQuery(SQL)) {
      ResultSetMetaData rsMetaData = rs.getMetaData();

      System.out.println("Column count: " + rsMetaData.getColumnCount());

      System.out.println("Column name: " + rsMetaData.getColumnName(1));
      System.out.println("Column label: " + rsMetaData.getColumnLabel(1));
      System.out.println("Column type: " + rsMetaData.getColumnType(1));
      System.out.println("Column type name: " + rsMetaData.getColumnTypeName(1));

      rs.close();
    }

    stmt.close();
    dbConn.close();
  }

  @Test
  public void testResultSet() throws SQLException {
    Connection dbConn = connectViaProperties();
    Statement stmt = dbConn.createStatement();

    String sql = "SELECT \"page\", \"__time\" "
        + "FROM \"wikiticker\" "
        + "LIMIT 1";
    try (ResultSet rs = stmt.executeQuery(sql)) {
      if (rs.next()) {
        System.out.println("getString(int): " + rs.getString(1));
        System.out.println("getString(String): " + rs.getString("page"));
        System.out.println("getTimestamp(int): " + rs.getTimestamp(2));
        System.out.println("getTime(int): " + rs.getTime(2));
        // TODO
//        System.out.println("getBytes(String): " + Arrays.toString(rs.getBytes(1)));
      }

      rs.close();
    }

    stmt.close();
    dbConn.close();
  }

  private static Connection connectViaUrl() throws SQLException {
    String url = "jdbc:cheetah:http:@default:"
        + CheetahCluster.BROKER_HOST + '=' + "10.214.208.42" + "&"
        + CheetahCluster.BROKER_PORT + '=' + "8082" + "&"
        + CheetahCluster.COORDINATOR_HOST + '=' + "10.214.208.42" + "&"
        + CheetahCluster.COORDINATOR_PORT + '=' + "8081";
    System.out.println(url);

    return DriverManager.getConnection(url, new Properties());
  }

  private static Connection connectViaProperties() throws SQLException {
    String url = "jdbc:cheetah:http:@default:";

    Properties props = new Properties();
    props.setProperty(CheetahCluster.BROKER_HOST, "10.214.208.42");
    props.setProperty(CheetahCluster.BROKER_PORT, "8082");
    props.setProperty(CheetahCluster.COORDINATOR_HOST, "10.214.208.42");
    props.setProperty(CheetahCluster.COORDINATOR_PORT, "8081");

    return DriverManager.getConnection(url, props);
  }

  @BeforeClass
  public static void setUp() throws ClassNotFoundException {
    Class.forName(CheetahDriver.class.getName());
  }

}
