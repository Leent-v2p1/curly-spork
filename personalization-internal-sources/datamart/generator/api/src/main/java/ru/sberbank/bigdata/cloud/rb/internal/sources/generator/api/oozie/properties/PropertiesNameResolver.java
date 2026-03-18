package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;

public class PropertiesNameResolver {

    private PropertiesNameResolver() {
        throw new IllegalStateException("Utility class");
    }

    public static String getSparkPropertiesTemplateName(String actionName) {
        return WorkflowNameEscaper.escape(actionName) + "-spark.template";
    }

    public static String getSystemPropertiesTemplateName(String actionName) {
        return WorkflowNameEscaper.escape(actionName) + "-system.template";
    }

    public static String getSparkPropertiesName(String actionName) {
        return WorkflowNameEscaper.escape(actionName) + "-properties-spark.conf";
    }

    public static String getSystemPropertiesName(String actionName) {
        return WorkflowNameEscaper.escape(actionName) + "-properties-system.conf";
    }
}
