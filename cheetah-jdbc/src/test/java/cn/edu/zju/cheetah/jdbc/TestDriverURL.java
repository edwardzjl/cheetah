/**
 * 
 */
package cn.edu.zju.cheetah.jdbc;

import java.util.Arrays;

import junit.framework.TestCase;

/**
 * @author JIANG
 *
 */
public class TestDriverURL extends TestCase {

  public void testDriverURL() throws Exception {
    String driverURL =
        "jdbc:cheetah:http:@default:broker.host=10.234.34.67&broker.port=8081&coordinator.host=10.234.45.67&coordinator.port=9081";
    String[] fields = driverURL.split(":");
    System.out.println(Arrays.toString(fields));
    String[] clusterSettings = fields[4].split("&");
    System.out.println(Arrays.toString(clusterSettings));
    String host = "10.214.208.59";
    String driverUrlStr = DriverURL.getDriverURL(host, 8082, host, 8081, host, 8090);
    System.out.println(driverUrlStr);
    DriverURL driverUrl = new DriverURL(driverUrlStr);
    System.out.println(driverUrl);
    assertEquals(host, driverUrl.getBrokerHost());
    assertEquals(8082, driverUrl.getBrokerPort());
    assertEquals(host, driverUrl.getCoordinatorHost());
    assertEquals(8081, driverUrl.getCoordinatorPort());
    assertEquals(host, driverUrl.getOverloadHost());
    assertEquals(8090, driverUrl.getOverloadPort());
  }

}
