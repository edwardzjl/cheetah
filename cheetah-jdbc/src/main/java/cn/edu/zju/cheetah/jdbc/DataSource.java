/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;

/**
 * @author David
 *
 */
public class DataSource {
  
  public static String DriverPrefix = "jdbc:cheetah:";
  
  private String server;
  
  private int port;
  
  private String database;

  public DataSource(String url) throws InvalidDataSourceException {
    checkNotNull(url);
    if (!url.startsWith(DriverPrefix))
      throw new InvalidDataSourceException();
    
    int uriStart = DriverPrefix.length();
    try {
      URI uri = URI.create(url.substring(uriStart));
      String host = uri.getHost();
      if(host == null)
        throw new InvalidDataSourceException("No server is specified!");
      int port = uri.getPort();
      if(port < 0)
        throw new InvalidDataSourceException("No server port is specified!");
      this.server = host;
      this.port = port;
      // TODO: add code for processing database
    } catch (IllegalArgumentException e) {
      throw new InvalidDataSourceException(e);
    }
  }
  
  public String getServer() {
    return this.server;
  }
  
  public int getPort() {
    return this.port;
  }
  
  public String getDatabase() {
    return this.database;
  }
}
