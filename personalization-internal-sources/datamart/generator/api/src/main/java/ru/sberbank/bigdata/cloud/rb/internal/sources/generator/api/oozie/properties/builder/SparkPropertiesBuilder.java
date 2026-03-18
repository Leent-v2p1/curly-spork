package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.HashMap;
import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonFiles.PERSONALIZATION_JAR;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper.getTemplate;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper.printTemplate;

public class SparkPropertiesBuilder<T extends ActionConf> {

    protected final String actionName;
    protected final T actionConf;
    protected final Environment env;
    protected final DatamartProperties properties;

    public SparkPropertiesBuilder(String actionName, T actionConf, Environment env, DatamartProperties properties) {
        this.actionName = actionName;
        this.actionConf = actionConf;
        this.env = env;
        this.properties = properties;
    }

    public String buildSparkProperties() {
        return printTemplate(getTemplate("datamart-spark-properties.ftl"), getAllParams());
    }

    public String buildSystemProperties() {
        return printTemplate(getTemplate("datamart-system-properties.ftl"), getAllParams());
    }

    protected MemoryParams getMemoryParams() {
        if (env.isTestEnvironment()) {
            return MemoryParams.minimalMemoryParams();
        }
        if (properties != null && properties.customJavaOpts().containsKey(PropertyConstants.CATALOG_ENUM_NAME)) {
            return MemoryParams.memoryParamsBasedOnActionConf(actionConf, MemoryParams.minimalMemoryParams());
        }
        return MemoryParams.memoryParamsBasedOnActionConf(actionConf, MemoryParams.defaultMemoryParams());
    }

    protected Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("jar", PERSONALIZATION_JAR);
        params.put("className", actionConf.getClassName());
        params.put("datamartClassName", properties.getClassName());
        params.put("resultSchema", properties.getTargetSchema());
        params.put("resultTable", properties.getTargetTable());
        params.put("sourceSchema", properties.getSourceSchema());
        params.put("workflowType", properties.getWorkflowType());
        params.put("datamartTableName", properties.parentDatamartName());
        params.put("monthlyDependencyDay", properties.getMonthlyDependencyDay());
        params.put("ctlEntityId", properties.getCtlEntityId().orElse(null));
        final Map<String, String> customJavaOpts = properties.customJavaOpts();
        params.put("customJavaOpts", addSparkPrefix(customJavaOpts).entrySet());
        return params;
    }

    protected Map<String, String> addSparkPrefix(Map<String, String> customJavaOpts) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : customJavaOpts.entrySet()) {
            String oldKey = entry.getKey();
            String newKey = oldKey.startsWith(PropertyConstants.SPARK_OPTION_PREFIX)
                    ? oldKey
                    : PropertyConstants.SPARK_OPTION_PREFIX + oldKey;
            result.put(newKey, entry.getValue());
        }
        return result;
    }

    private Map<String, Object> getAllParams() {
        Map<String, Object> params = getParams();
        params.putAll(getMemoryParams().toMap());
        return params;
    }
}
