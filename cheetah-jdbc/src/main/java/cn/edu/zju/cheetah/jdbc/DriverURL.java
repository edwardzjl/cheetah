/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author David
 *
 */
public class DriverURL {

  public static String DriverPrefix = "jdbc:cheetah:";

  public static String DriverProtocol = "http";

  private String database;

  public DriverURL(String url) throws InvalidDriverURLException {
    checkNotNull(url);
    String[] parts = url.split(":");
    if (parts.length != 4)
      throw new InvalidDriverURLException("DriverURL: jdbc:cheetah:<protocol>:@<database>");

    if (!(parts[0].equals("jdbc") && parts[1].equals("cheetah")))
      throw new InvalidDriverURLException();
    if (!parts[2].equals("http"))
      throw new InvalidDriverURLException();
    if (!parts[3].equals("@default")) // TODO: replace it with real database
      throw new InvalidDriverURLException();
   
    this.database = parts[3].substring(1);
  }

  public String getDatabase() {
    return this.database;
  }
}
