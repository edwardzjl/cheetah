package cn.edu.zju.cheetah.jdbc;

import java.util.Map;

public class CheetahCluster {

  public static final String PROTOCOL = "protocol";

  public static final String DATABASE = "database";

  public static final String BROKER_HOST = "broker.host";
  
  public static final String BROKER_PORT = "broker.port";
  
  public static final String COORDINATOR_HOST = "coordinator.host";
  
  public static final String COORDINATOR_PORT = "coordinator.port";
  
  public static final String OVERLORD_HOST = "overlord.host";
  
  public static final String OVERLORD_PORT = "overlord.port";

  public static final String[] PROPERTY_NAMES = new String[] {
      BROKER_HOST, BROKER_PORT,
      COORDINATOR_HOST, COORDINATOR_PORT,
      OVERLORD_HOST, OVERLORD_PORT,
  };

  public static String getBroker(Map<String, Object> properties) {
    if (!properties.containsKey(PROTOCOL)) {
      throw new IllegalArgumentException("Protocol is not set");
    }
    if (!properties.containsKey(BROKER_HOST)) {
      throw new IllegalArgumentException("Host of broker is not set");
    }
    if (!properties.containsKey(BROKER_PORT)) {
      throw new IllegalArgumentException("Port of broker is not set");
    }
    return properties.get(PROTOCOL) + "://"
        + properties.get(BROKER_HOST) + ':' + properties.get(BROKER_PORT);
  }

  public static String getCoordinator(Map<String, Object> properties) {
    if (!properties.containsKey(PROTOCOL)) {
      throw new IllegalArgumentException("Protocol is not set");
    }
    if (!properties.containsKey(COORDINATOR_HOST)) {
      throw new IllegalArgumentException("Host of coordinator is not set");
    }
    if (!properties.containsKey(COORDINATOR_PORT)) {
      throw new IllegalArgumentException("Port of coordinator is not set");
    }
    return properties.get(PROTOCOL) + "://"
        + properties.get(COORDINATOR_HOST) + ':' + properties.get(COORDINATOR_PORT);
  }

  public static String getOverlord(Map<String, Object> properties) {
    if (!properties.containsKey(PROTOCOL)) {
      throw new IllegalArgumentException("Protocol is not set");
    }
    if (!properties.containsKey(OVERLORD_HOST)) {
      throw new IllegalArgumentException("Host of overlord is not set");
    }
    if (!properties.containsKey(OVERLORD_PORT)) {
      throw new IllegalArgumentException("Port of overlord is not set");
    }
    return properties.get(PROTOCOL) + "://"
        + properties.get(OVERLORD_HOST) + ':' + properties.get(OVERLORD_PORT);
  }

}
