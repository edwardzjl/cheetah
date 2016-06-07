/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Driver;
import java.sql.DriverManager;

import junit.framework.TestCase;

/**
 * @author David
 *
 */
public class TestCheetahDriver extends TestCase {

  public void testCheetahDriver() throws Exception {
    String dbUrl = "jdbc:cheetah://localhost:8080/sampleDB";
    Class.forName(CheetahDriver.class.getName());
    Driver driver = DriverManager.getDriver(dbUrl);
    assertNotNull(driver);
  }
  
}
