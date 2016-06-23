/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
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
    String sql = "select * from table";
    Class.forName(CheetahDriver.class.getName());
    
    Properties cheetah = new Properties();
    cheetah.setProperty(CheetahCluster.BROKER_HOST, "localhost");
    cheetah.setProperty(CheetahCluster.BROKER_PORT, "8023");
    cheetah.setProperty(CheetahCluster.COORDINATOR_HOST, "localhost");
    cheetah.setProperty(CheetahCluster.COORDINATOR_PORT, "9078");
    cheetah.setProperty(CheetahCluster.OVERLOAD_HOST, "localhost");
    cheetah.setProperty(CheetahCluster.OVERLOAD_PORT, "7856");
    
    Connection dbConn = DriverManager.getConnection(dbUrl, cheetah);
//    DatabaseMetaData dbmd = dbConn.getMetaData();
//    dbmd.getTables(catalog, schemaPattern, tableNamePattern, types);
//    dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    Statement stmt = dbConn.createStatement();
    stmt.executeQuery(sql);
    System.out.println(dbConn);
    System.out.println(stmt);
    System.out.println(dbConn.getCatalog());
  }

}
