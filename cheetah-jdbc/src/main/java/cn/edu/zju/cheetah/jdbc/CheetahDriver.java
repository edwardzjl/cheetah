/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
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

  public static String DriverName = "ZJU Cheetah JDBC Driver";

  static {
    try {
      DriverManager.registerDriver(new CheetahDriver());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return new CheetahConnection(new DataSource(checkNotNull(url)), checkNotNull(info));
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    try {
      new DataSource(url);
    } catch (InvalidDataSourceException e) {
      return false;
    }
    return true;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    throw new UnsupportedOperationException();
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
