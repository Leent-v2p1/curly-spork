package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.lineSeparator;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.ALLOW_FOR_ALL_PREFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.PROPERTY_SEPARATOR;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.SPARK_OPTION_PREFIX;

/**
 * Class find all placeholders like datamart.table_name.param_name in text
 * * table_name - is variable part which represents target table of current sub-workflow
 * * and thus linking custom params with sub-workflow
 * * param_name - is variable part. It can be one of spark framework parameters that start from spark.*
 * * or it may have fully custom name to use it significantly at datamart code
 */
public class CustomSetter extends ParamSetter {
    private final Logger log = LoggerFactory.getLogger(CustomSetter.class);
    private final Map<String, String> wfParam;
    private final String datamartPrefix;
    //check that property looks like datamart.витрина.param or datamart.витрина.spark.param
    private final Pattern propertyPattern;

    public CustomSetter(Map<String, String> wfParam, String tableName) {
        this.wfParam = wfParam;
        this.datamartPrefix = String.format("datamart.%s.", tableName);
        this.propertyPattern = Pattern.compile(datamartPrefix + ParamSetterPatterns.NON_EXECUTION_PARAMS);
    }

    @Override
    public String run(String allProperty) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> property : wfParam.entrySet()) {
            String param = resolveParam(property.getKey());
            String value = property.getValue();
            if (param.isEmpty() || value == null || value.isEmpty()) {
                continue;
            }

            stringBuilder.append(SPARK_OPTION_PREFIX)
                    .append(param)
                    .append(PROPERTY_SEPARATOR)
                    .append(value)
                    .append(lineSeparator());
        }
        return replaceFirst(allProperty, stringBuilder.toString(), ParamSetterPatterns.CUSTOM_PARAM.getPattern());
    }

    @VisibleForTesting
    protected String resolveParam(String property) {
        if (property.startsWith(ALLOW_FOR_ALL_PREFIX)) {
            Matcher matcher = ParamSetterPatterns.ALL_TABLES_NONEXECUTION.getPattern().matcher(property);
            if (matcher.matches()) {
                return getClearParam(property, ALLOW_FOR_ALL_PREFIX);
            }
        }
        if (property.startsWith(datamartPrefix)) {
            Matcher matcher = propertyPattern.matcher(property);
            if (matcher.matches()) {
                return getClearParam(property, datamartPrefix);
            }
        }
        return StringUtils.EMPTY;
    }

    private String getClearParam(final String param, final String tablePrefix) {
        int tablePrefixLength = tablePrefix.length();
        if (param.startsWith(tablePrefix + SPARK_OPTION_PREFIX)) {
            return param.substring(tablePrefixLength + SPARK_OPTION_PREFIX.length());
        }
        return param.substring(tablePrefixLength);
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
