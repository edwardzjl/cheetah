/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import junit.framework.TestCase;

/**
 * @author David
 *
 */
public class TestJDBC extends TestCase {

  public void testCheetahJDBC() throws Exception {
    String dbUrl = "jdbc:cheetah://localhost:8080/sampleDB";
    String sql = "select * from table";
    Class.forName(CheetahDriver.class.getName());
    Connection dbConn = DriverManager.getConnection(dbUrl);
    Statement stmt = dbConn.createStatement();
    stmt.executeQuery(sql);
    System.out.println(dbConn);
    System.out.println(stmt);
  }

}
