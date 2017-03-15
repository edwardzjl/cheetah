package cn.edu.zju.cheetah.jdbc;


import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.remote.AvaticaHttpClientFactoryImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

/**
 * Created by edwardlol on 17-3-14.
 */
public enum CheetahConnectionProperty implements ConnectionProperty {
    //~ Enum members -----------------------------------------------------------

    PROTOCOL("protocal", Type.STRING, "http", true),

    DATABASE("database", Type.STRING, null, false),

    BROKER_HOST("brokerHost", Type.STRING, "localhost", false),

    BROKER_PORT("brokerPort", Type.STRING, "8082", false),

    COORDINATOR_HOST("coordinatorHost", Type.STRING, "localhost", false),

    COORDINATOR_PORT("coordinatorPort", Type.STRING, "8081", false),

    OVERLORD_HOST("overlordHost", Type.STRING, "localhost", false),

    OVERLORD_PORT("overlordPort", Type.STRING, "8090", false);

    //~ Static fields ----------------------------------------------------------

    private static final Map<String, CheetahConnectionProperty> NAME_TO_PROPS;

    static {
        NAME_TO_PROPS = new HashMap<>();
        for (CheetahConnectionProperty p : CheetahConnectionProperty.values()) {
            NAME_TO_PROPS.put(p.camelName.toUpperCase(), p);
            NAME_TO_PROPS.put(p.name(), p);
        }
    }

    //~ Instance fields --------------------------------------------------------

    private final String camelName;

    private final Type type;

    private final Object defaultValue;

    private final boolean required;

    //~ Constructors -----------------------------------------------------------

    CheetahConnectionProperty(String camelName, Type type, Object defaultValue, boolean required) {
        this.camelName = camelName;
        this.type = type;
        this.defaultValue = defaultValue;
        this.required = required;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public ConnectionConfigImpl.PropEnv wrap(Properties properties) {
        return new ConnectionConfigImpl.PropEnv(parse(properties, NAME_TO_PROPS), this);
    }

    @Override
    public String camelName() {
        return this.camelName;
    }

    @Override
    public Type type() {
        return this.type;
    }

    @Override
    public Object defaultValue() {
        return this.defaultValue;
    }

    @Override
    public boolean required() {
        return this.required;
    }
}
