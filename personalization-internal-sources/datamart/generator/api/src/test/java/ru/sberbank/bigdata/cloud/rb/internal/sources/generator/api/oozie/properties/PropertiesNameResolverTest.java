package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertiesNameResolverTest {

    private final String DATAMART_NAME = "custom_rb_xx.datamart";

    @Test
    void testGetSparkPropertiesTemplateName() {
        String sparkPropertiesTemplateName = PropertiesNameResolver.getSparkPropertiesTemplateName(DATAMART_NAME);
        assertEquals("custom-rb-xx-datamart-spark.template", sparkPropertiesTemplateName);
    }

    @Test
    void testGetSystemPropertiesTemplateName() {
        String systemPropertiesTemplateName = PropertiesNameResolver.getSystemPropertiesTemplateName(DATAMART_NAME);
        assertEquals("custom-rb-xx-datamart-system.template", systemPropertiesTemplateName);
    }

    @Test
    void testGetSparkPropertiesName() {
        String systemPropertiesTemplateName = PropertiesNameResolver.getSparkPropertiesName(DATAMART_NAME);
        assertEquals("custom-rb-xx-datamart-properties-spark.conf", systemPropertiesTemplateName);
    }

    @Test
    void testGetSystemPropertiesName() {
        String systemPropertiesTemplateName = PropertiesNameResolver.getSystemPropertiesName(DATAMART_NAME);
        assertEquals("custom-rb-xx-datamart-properties-system.conf", systemPropertiesTemplateName);
    }
}
