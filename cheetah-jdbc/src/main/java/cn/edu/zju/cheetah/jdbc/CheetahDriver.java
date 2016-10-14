package cn.edu.zju.cheetah.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahSchemaFactory;

public class CheetahDriver implements Driver {

  static {
    try {
      DriverManager.registerDriver(new CheetahDriver());
    } catch (SQLException e) {
      throw new RuntimeException("Cannot load Cheetah driver");
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    Map<String, Object> operand = convertProps(url, info);

    Connection connection = DriverManager.getConnection("jdbc:calcite:", new Properties());
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

    CheetahSchemaFactory schemaFactory = new CheetahSchemaFactory();
    Schema schema = schemaFactory.create(calciteConnection.getRootSchema(), "default", operand);

    calciteConnection.getRootSchema().add("default", schema);
    calciteConnection.setSchema("default");
    return calciteConnection;
  }

  private Map<String, Object> convertProps(String url, Properties connProps) {
    Map<String, Object> props = new HashMap<>();

    String[] urlParts = url.split(":");
    if (urlParts.length < 4)
      throw new IllegalArgumentException(
          "URL form: jdbc:cheetah:<protocol>:@<database>, but actual: \"" + url + "\"");
    if (!urlParts[2].equals("http"))
      throw new IllegalArgumentException("URL: Only support http protocol");
    if (!urlParts[3].equals("@default"))
      throw new IllegalArgumentException("URL: Only support database 'default'");

    props.put(CheetahCluster.PROTOCOL, urlParts[2]);
    props.put(CheetahCluster.DATABASE, urlParts[3]);

    if (urlParts.length > 4) {
      Map<String, String> parameters = new HashMap<>();
      String[] parts = urlParts[4].split("&");
      for(String parameter : parts) {
        String[] pair = parameter.split("=");
        parameters.put(pair[0], pair[1]);
      }

      for (String propertyName : CheetahCluster.PROPERTY_NAMES)
        if (parameters.containsKey(propertyName)){
          props.put(propertyName, parameters.get(propertyName));
        }
    } else {
      for (String propertyName : CheetahCluster.PROPERTY_NAMES)
        if (connProps.containsKey(propertyName)){
          props.put(propertyName, connProps.get(propertyName));
        }
    }

    Map<String, Object> operand = new HashMap<>();
    operand.put("url", CheetahCluster.getBroker(props));
    operand.put("coordinatorUrl", CheetahCluster.getCoordinator(props));
    return operand;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url == null) return false;
    String[] parts = url.split(":");
    System.out.println(parts.length);
    return parts.length >= 2
        && parts[0].equals("jdbc")
        && parts[1].equals("cheetah");
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    throw new SQLException();
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
    throw new SQLFeatureNotSupportedException();
  }

}
