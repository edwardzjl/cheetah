/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author David
 *
 */
public class CheetahDriver implements Driver {
  
  public static String DriverPrefix = "jdbc:cheetah";
  
  public static String DriverName = "ZJU Cheetah JDBC Driver";

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if(!url.startsWith(DriverPrefix))
      return false;
    return true;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return null;
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public boolean jdbcCompliant() {
    return true;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new UnsupportedOperationException();
  }

}
