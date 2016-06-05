/**
 * 
 */
package cn.edu.zju.cheetah.sandbox.david;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 * @author JIANG
 *
 */
public class TestDerby {

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    String dbUrl = "jdbc:derby:memory:demo;create=true";
    Driver derbyDriver = (Driver) Class.forName(driver).newInstance();
    System.out.println("getMajorVersion(): " + derbyDriver.getMajorVersion());
    System.out.println("getMinorVersion(): " + derbyDriver.getMinorVersion());
    System.out.println("jdbcCompliant(): " + derbyDriver.jdbcCompliant());
    System.out.println("acceptsURL(): " + derbyDriver.acceptsURL(dbUrl));
    dbUrl = "jdbc:derby::demo;create=true";
    System.out.println("acceptsURL(): " + derbyDriver.acceptsURL(dbUrl));
    dbUrl = "jdbc:derby:memory:demo;create=true";
    Connection conn = DriverManager.getConnection(dbUrl);
    System.out.println("getCatalog(): " + conn.getCatalog());
    DatabaseMetaData metaData = conn.getMetaData();
    System.out.println("getDatabaseMajorVersion(): " + metaData.getDatabaseMajorVersion());
    System.out.println("getDatabaseMinorVersion(): " + metaData.getDatabaseMinorVersion());
    System.out.println("getDatabaseProductName(): " + metaData.getDatabaseProductName());
    System.out.println("getDatabaseProductVersion(): " + metaData.getDatabaseProductVersion());
    System.out.println("getDriverName(): " + metaData.getDriverName());
    System.out.println(metaData.getSQLKeywords());
    System.out.println(metaData.getCatalogSeparator());
    System.out.println("End");
    try(ResultSet rs = metaData.getCatalogs()) {
      while(rs.next())
        System.out.println(rs.getString(1));
    }
  }

}
