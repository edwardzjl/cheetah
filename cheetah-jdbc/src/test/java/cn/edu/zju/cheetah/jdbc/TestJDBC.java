/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestCase;

/**
 * @author David
 *
 */
public class TestJDBC extends TestCase {

  public void testCheetahJDBC() throws Exception {
    String dbUrl = "jdbc:cheetah:http:@default";
    String tableName = "metrics";
    String sql = "SELECT * FROM " + tableName
        + " WHERE interval BETWEEN '2016-01-01' AND '2017-01-01' LIMIT 100";
    Class.forName(CheetahDriver.class.getName());

    Properties cheetah = new Properties();
    cheetah.setProperty(CheetahCluster.BROKER_HOST, "10.214.208.59");
    cheetah.setProperty(CheetahCluster.BROKER_PORT, "8082");
    cheetah.setProperty(CheetahCluster.COORDINATOR_HOST, "10.214.208.59");
    cheetah.setProperty(CheetahCluster.COORDINATOR_PORT, "8081");
    cheetah.setProperty(CheetahCluster.OVERLORD_HOST, "10.214.208.59");
    cheetah.setProperty(CheetahCluster.OVERLORD_PORT, "8090");

    Connection dbConn = DriverManager.getConnection(dbUrl, cheetah);
    System.out.println(dbConn);
    System.out.println(dbConn.getCatalog());

    DatabaseMetaData dbmd = dbConn.getMetaData();
    ResultSet rs;
    rs = dbmd.getTables(null, null, null, null);
    System.out.println("Tables: ");
    while (rs.next()) {
      System.out.println(rs.getString(1));
    }
    rs.close();
    rs = dbmd.getColumns(null, null, tableName, null);
    System.out.println("Columns of table [" + tableName + "]: ");
    while (rs.next()) {
      System.out.println(rs.getString(2));
    }
    rs.close();

    Statement stmt = dbConn.createStatement();
    System.out.println(stmt);
    rs = stmt.executeQuery(sql);
    System.out.println("Result of SQL \"" + sql + "\": " + rs.getRow());
    while (rs.next()) {
      for (int i = 1; i <= 11; i++) {
        System.out.print(rs.getString(i) + " | ");
      }
      System.out.println();
    }
    rs.close();
  }

}
