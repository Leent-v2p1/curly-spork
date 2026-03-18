package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.Map;

public class ReservingProperties extends DatamartProperties {
    public final String driverMemory;
    public final Map<String, String> javaOpts;

    public ReservingProperties(FullTableName datamartId, Map<String, Object> customProperties, String driverMemory, Map<String, String> javaOpts) {
        super(datamartId, customProperties);
        this.driverMemory = driverMemory;
        this.javaOpts = javaOpts;
    }

    @Override
    public String getDriverMemory() {
        return driverMemory;
    }

    @Override
    public Map<String, String> customJavaOpts() {
        return javaOpts;
    }

}
