package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties;

import java.util.Map;

public class Properties {

    private static Map<String, Object> propertyMap;

    public static Map<String, Object> get() {
        if (propertyMap == null) {
            propertyMap = new YamlPropertiesParser().parse("/datamart-properties.yaml");
        }
        return propertyMap;
    }
}
