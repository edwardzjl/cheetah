package cn.edu.zju.cheetah.jdbc;

import cn.edu.zju.cheetah.jdbc.adapter.CheetahSchemaFactory;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.ConversionUtil;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class CheetahDriver implements Driver {
    //~ Static fields, methods and block ---------------------------------------

    /**
     * log handler
     */
    private static final org.apache.log4j.Logger LOG =
            org.apache.log4j.Logger.getLogger(CheetahDriver.class);

    /**
     * method to init the charset so that cheetah can support chinese characters
     */
    private static void initCharset() {
        System.setProperty("saffron.default.charset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.nationalcharset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.collation.name", ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");
    }

    static {
        try {
            DriverManager.registerDriver(new CheetahDriver());
            initCharset();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot load Cheetah driver");
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Attempts to make a database connection to the given URL.
     *
     * @param url  the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as
     *             connection arguments. Normally at least a "user" and
     *             "password" property should be included.
     * @return a <code>Connection</code> object that represents a connection to the URL
     * @throws SQLException if a database access error occurs or the url is
     *                      {@code null}
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        LOG.debug("Jdbc url: " + url);
        LOG.debug("Jdbc props: " + info);

        StringBuilder sb = new StringBuilder();
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            sb.append(drivers.nextElement().getClass().getName()).append(',').append(' ');
        }
        // delete the last ", "
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
        }
        LOG.debug("Installed jdbc drivers: " + sb.toString());

        Map<String, Object> operand = convertProps(url, info);

        Connection connection = DriverManager.getConnection("jdbc:calcite:", new Properties());
        LOG.debug("Connection " + connection + " from " + url);

        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        CheetahSchemaFactory schemaFactory = new CheetahSchemaFactory();
        Schema schema = schemaFactory.create(calciteConnection.getRootSchema(), "default", operand);

        calciteConnection.getRootSchema().add("default", schema);
        calciteConnection.setSchema("default");
        return calciteConnection;
    }

    /**
     * convert the properties from the connection arguments into a map
     *
     * @param url  the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as
     *             connection arguments. Normally at least a "user" and
     *             "password" property should be included.
     *             example:
     *             jdbc:cheetah:<protocol>:@<database>[:param1_name=param1_value&param2_name=param2_value]
     * @return a map contains converted properties
     */
    private Map<String, Object> convertProps(String url, Properties info) {
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

        // edwardlol:
        // check the connection arguments for props first,
        for (String propertyName : CheetahCluster.PROPERTY_NAMES) {
            if (info.containsKey(propertyName)) {
                props.put(propertyName, info.get(propertyName));
            }
        }
        // if the url overwites the properties, use the url props
        if (urlParts.length > 4) {
            Map<String, String> params = new HashMap<>();
            String[] kvs = urlParts[4].split("&");
            for (String kv : kvs) {
                String[] pair = kv.split("=");
                params.put(pair[0], pair[1]);
            }
            for (String propertyName : CheetahCluster.PROPERTY_NAMES) {
                if (params.containsKey(propertyName)) {
                    props.put(propertyName, params.get(propertyName));
                }
            }
        }

        Map<String, Object> operand = new HashMap<>();
        operand.put("url", CheetahCluster.getBroker(props));
        operand.put("coordinatorUrl", CheetahCluster.getCoordinator(props));
        return operand;
    }

    /**
     * Retrieves whether the driver thinks that it can open a connection
     * to the given URL.  Typically drivers will return <code>true</code> if they
     * understand the sub-protocol specified in the URL and <code>false</code> if
     * they do not.
     *
     * @param url the URL of the database
     * @return <code>true</code> if this driver understands the given URL;
     *         <code>false</code> otherwise
     * @exception SQLException if a database access error occurs or the url is
     * {@code null}
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null)
            return false;

        String[] parts = url.split(":");
        System.out.println("num of parts in url: " + parts.length);
        return parts.length >= 2
                && parts[0].equals("jdbc")
                && parts[1].equals("cheetah");
    }

    /**
     * Gets information about the possible properties for this driver.
     * <P>
     * The <code>getPropertyInfo</code> method is intended to allow a generic
     * GUI tool to discover what properties it should prompt
     * a human for in order to get
     * enough information to connect to a database.  Note that depending on
     * the values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate though several calls
     * to the <code>getPropertyInfo</code> method.
     *
     * @param url the URL of the database to which to connect
     * @param info a proposed list of tag/value pairs that will be sent on
     *          connect open
     * @return an array of <code>DriverPropertyInfo</code> objects describing
     *          possible properties.  This array may be an empty array if
     *          no properties are required.
     * @exception SQLException if a database access error occurs
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // TODO: 17-3-14 finish this
//        throw new SQLException();

        List<DriverPropertyInfo> list = new ArrayList<>();

        // First, add the contents of info
        for (Map.Entry<Object, Object> entry : info.entrySet()) {
            list.add(new DriverPropertyInfo((String) entry.getKey(), (String) entry.getValue()));
        }
        // Next, add property definitions not mentioned in info
        for (ConnectionProperty p : getConnectionProperties()) {
            if (info.containsKey(p.name())) {
                continue;
            }
            list.add(new DriverPropertyInfo(p.name(), null));
        }
        return list.toArray(new DriverPropertyInfo[list.size()]);
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
