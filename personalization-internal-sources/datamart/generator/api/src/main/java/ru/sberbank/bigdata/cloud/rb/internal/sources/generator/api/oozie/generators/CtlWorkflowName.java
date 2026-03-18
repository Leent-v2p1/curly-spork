package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CtlWorkflowName {

    public static final String TEST_PREFIX = "test";
    public static final String PROJECT_NAME_MASSPERS = "masspers";
    public static final String ANSIBLE_PIPELINE_PREFIX_TEMPLATE = "{{common.ctl_v5.workflow.prefix}}";
    public static final String USER_HOLDER_TEMPLATE = "{{security.%s.username}}";
    public static final String QUEUE_TEMPLATE = "{{common.yarn.%s.queue}}";
    public static final String KEYTAB_HOLDER_TEMPLATE = "{{security.%s.keytab.path}}";
    public static final String DATAMARTS = "datamarts";
    public static final String WFSYAML = "wfs.yaml";

    public static String getCtlWorkflowName(Environment environment, String schemaAlias, String nameModifier) {
        final String test = environment.isTestEnvironment() ? TEST_PREFIX : "";
        return Stream.of(PROJECT_NAME_MASSPERS, ANSIBLE_PIPELINE_PREFIX_TEMPLATE, test, schemaAlias, nameModifier, DATAMARTS)
                .filter(str -> !str.isEmpty())
                .collect(Collectors.joining("-"));
    }

    public static String getWfsFileName(Environment environment, String alias) {
        final String test = environment.isTestEnvironment() ? TEST_PREFIX : "";
        return Stream.of(test, alias, WFSYAML)
                .filter(str -> !str.isEmpty())
                .collect(Collectors.joining("-"));
    }

    public static String getCtlWorkflowUserHolder(String alias) {
        return USER_HOLDER_TEMPLATE.replaceFirst("%s", alias);
    }

    public static String getCtlWorkflowKeytabHolder(String alias) {
        return KEYTAB_HOLDER_TEMPLATE.replaceFirst("%s", alias);
    }

    public static String getCtlWorkflowQueue(String alias) {
        return QUEUE_TEMPLATE.replaceFirst("%s", alias);
    }
}
