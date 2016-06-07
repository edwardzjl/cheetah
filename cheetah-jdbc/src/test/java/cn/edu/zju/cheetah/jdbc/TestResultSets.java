/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;

import junit.framework.TestCase;

/**
 * @author David
 *
 */
public class TestResultSets extends TestCase {
  
  public void testResultSets() throws Exception {
    String dbUrl = "jdbc:derby:memory:demo;create=true";
    String sql = "CREATE TABLE HY_Address (ID INT, StreetName VARCHAR(20), City VARCHAR(20))";
    try (Connection dbConn = DriverManager.getConnection(dbUrl)) {
      dbConn.createStatement().execute(sql);
      DatabaseMetaData dbMeta = dbConn.getMetaData();
      String str = ResultSets.toString(dbMeta.getTables(null, null, null, null));
      System.out.println(str);
    }
  }

}
