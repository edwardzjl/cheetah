/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

/**
 * @author David
 *
 */
public class DriverURL {

  public static String DriverPrefix = "jdbc:cheetah:";

  public static String DriverProtocol = "http";

  private String database;
  
  private String bHost;
  
  private int bPort;

  private String oHost;

  private int oPort;

  private String cHost;
  
  private int cPort;

  public static String getDriverURL(String bHost, int bPort, String cHost, int cPort, String oHost,
      int oPort) {
    return getDriverURL("default", bHost, bPort, cHost, cPort, oHost, oPort);
  }

  public static String getDriverURL(String database, String bHost, int bPort, String cHost,
      int cPort, String oHost, int oPort) {
    StringBuilder builder = new StringBuilder();
    builder.append(DriverPrefix).append(DriverProtocol + ":").append("@" + database + ":")
        .append(CheetahCluster.BROKER_HOST + "=" + bHost + "&")
        .append(CheetahCluster.BROKER_PORT + "=" + bPort + "&")
        .append(CheetahCluster.COORDINATOR_HOST + "=" + cHost + "&")
        .append(CheetahCluster.COORDINATOR_PORT + "=" + cPort + "&")
        .append(CheetahCluster.OVERLORD_HOST + "=" + oHost + "&")
        .append(CheetahCluster.OVERLORD_PORT + "=" + oPort);
    return builder.toString();
  }

  public DriverURL(String url) throws InvalidDriverURLException {
    checkNotNull(url);
    String[] parts = url.split(":");
    if (parts.length < 4)
      throw new InvalidDriverURLException("DriverURL: jdbc:cheetah:<protocol>:@<database>");
    String[] props = null;
    if (parts.length > 4) {
      props = parts[4].split("&");
      if (props.length != 6)
        throw new InvalidDriverURLException(
            "DriverURL: jdbc:cheetah:<protocol>:@<database>:parameters");
    }

    if (!(parts[0].equals("jdbc") && parts[1].equals("cheetah")))
      throw new InvalidDriverURLException();
    if (!parts[2].equals("http"))
      throw new InvalidDriverURLException();
    if (!parts[3].equals("@default")) // TODO: replace it with real database
      throw new InvalidDriverURLException();
    this.database = parts[3].substring(1);
    
    if(props != null) {
      Map<String, String> kvs = new HashMap<>();
      for(String parameter : props) {
        String[] pair = parameter.split("=");
        kvs.put(pair[0], pair[1]);
      }
      this.bHost = kvs.get(CheetahCluster.BROKER_HOST);
      this.bPort = Integer.parseInt(kvs.get(CheetahCluster.BROKER_PORT));
      this.cHost = kvs.get(CheetahCluster.COORDINATOR_HOST);
      this.cPort = Integer.parseInt(kvs.get(CheetahCluster.COORDINATOR_PORT));
      this.oHost = kvs.get(CheetahCluster.OVERLORD_HOST);
      this.oPort = Integer.parseInt(kvs.get(CheetahCluster.OVERLORD_PORT));
    }
  }

  public String getDatabase() {
    return this.database;
  }
  
  public String getBrokerHost() {
    return this.bHost;
  }
  
  public int getBrokerPort() {
    return this.bPort;
  }
  
  public String getCoordinatorHost() {
    return this.cHost;
  }
  
  public int getCoordinatorPort() {
    return this.cPort;
  }
  
  public String getOverloadHost() {
    return this.oHost;
  }
  
  public int getOverloadPort() {
    return this.oPort;
  }

  @Override
  public String toString() {
    return getDriverURL(bHost, bPort, cHost, cPort, oHost, oPort);
  }
  
}
