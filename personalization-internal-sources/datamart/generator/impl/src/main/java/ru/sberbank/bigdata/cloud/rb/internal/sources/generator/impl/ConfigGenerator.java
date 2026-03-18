package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.Checker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.StringUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.ConfigReader;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.CtlParameters;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.ParamBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.ParamSetterRunner;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter.ParamSetter;

import java.util.List;
import java.util.Map;

/**
 * Class generates in runtime 2 files of parameters, which were collected by different ParamSetter:
 * ...properties-system.conf
 * ...properties-spark.conf
 * uses as input:
 * ...properties-system.template
 * ...properties-spark.template
 */
public class ConfigGenerator {
    private static final Logger log = LoggerFactory.getLogger(ConfigGenerator.class);
    private static final String PROPERTY_POSTFIX = ".conf";
    private static final String TEMPLATE_POSTFIX = ".template";

    private final ParamSetterRunner runner;

    public ConfigGenerator(ParamSetterRunner runner) {
        this.runner = runner;
    }

    protected static List<ParamSetter> buildSparkProperties(Map<String, String> ctlParam, String tableName) {
        return ParamBuilder.builder()
                .withCommon(ctlParam)
                .withExecute(ctlParam, tableName)
                .result();
    }

    protected static List<ParamSetter> buildSystemProperties(Map<String, String> ctlParam, String tableName) {
        return ParamBuilder.builder()
                .withSparkDefault()
                .withCommon(ctlParam)
                .withCustom(ctlParam, tableName)
                .result();
    }

    protected static String getValidPropertiesPath(String propertyName) {
        String propertyPath = SysPropertyTool.safeSystemProperty(propertyName);
        Checker.checkCondition(!propertyPath.endsWith(TEMPLATE_POSTFIX),
                "Spark template path should ends with %s", TEMPLATE_POSTFIX);
        return propertyPath;
    }

    /**
     * create new conf file based on template file 'propertyPath' content and name
     * Example:
     * input template: root/file.template
     * output conf file: root/file.conf
     */
    public void generatePropertyFile(List<ParamSetter> setters, String propertyPath) {
        log.info("reading template for property from {}", propertyPath);
        String propertyTemplate = HDFSHelper.readFileAsString(propertyPath);
        String resultProperty = runner.run(setters, propertyTemplate);
        String confFile = StringUtils.replaceLast(propertyPath, TEMPLATE_POSTFIX, PROPERTY_POSTFIX);
        log.info("writing result properties to {}", confFile);
        HDFSHelper.rewriteFile(resultProperty, confFile);
    }

    public static void main(String[] args) {
        final String systemPropertiesPath = getValidPropertiesPath("spark.system.property.path");
        final String sparkPropertiesPath = getValidPropertiesPath("spark.property.path");
        final ConfigReader configReader = new ConfigReader();
        final String tableName = configReader.getTableNameFromConfig(systemPropertiesPath);

        final CtlParameters ctlParametes = new CtlParameters(WfParam.createFromSystemProperties());
        final Map<String, String> parametersForSetters = ctlParametes.buildCtlParam();
        final List<ParamSetter> paramSettersSystem = buildSystemProperties(parametersForSetters, tableName);
        final List<ParamSetter> paramSettersSpark = buildSparkProperties(parametersForSetters, tableName);

        final ConfigGenerator configGenerator = new ConfigGenerator(new ParamSetterRunner());
        //spark
        configGenerator.generatePropertyFile(paramSettersSpark, sparkPropertiesPath);
        //system
        configGenerator.generatePropertyFile(paramSettersSystem, systemPropertiesPath);
    }
}
