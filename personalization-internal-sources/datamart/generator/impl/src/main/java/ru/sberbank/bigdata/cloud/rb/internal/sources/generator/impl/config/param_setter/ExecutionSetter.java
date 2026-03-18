package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.lineSeparator;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.ALLOW_FOR_ALL_PREFIX;

/**
 * Class find all placeholders like datamart.table_name.param_name in text
 * * table_name - is variable part which represents target table of current sub-workflow
 * * and thus linking executions params with sub-workflow
 * * param_name - is variable part, one of executorMemory|executors|executorCoreNum|driverMemory
 */
public class ExecutionSetter extends ParamSetter {
    private final Logger log = LoggerFactory.getLogger(ExecutionSetter.class);

    private final Map<String, String> wfParam;
    private final String datamartPrefix;
    //check that property looks like datamart.витрина.executorMemory or another param
    private final Pattern propertyPattern;

    public ExecutionSetter(Map<String, String> wfParam, String tableName) {
        this.wfParam = wfParam;
        this.datamartPrefix = String.format("datamart.%s.", tableName);
        this.propertyPattern = Pattern.compile(datamartPrefix + ParamSetterPatterns.EXECUTION_PARAMS);
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

            stringBuilder.append(param)
                    .append(PropertyConstants.PROPERTY_SEPARATOR)
                    .append(value)
                    .append(lineSeparator());
        }
        return replaceFirst(allProperty, stringBuilder.toString(), ParamSetterPatterns.CTL_EXECUTION.getPattern());
    }

    @VisibleForTesting
    protected String resolveParam(String property) {
        if (property.startsWith(ALLOW_FOR_ALL_PREFIX)) {
            Matcher matcher = ParamSetterPatterns.ALL_TABLES_EXECUTION.getPattern().matcher(property);
            if (matcher.matches()) {
                return property.substring(ALLOW_FOR_ALL_PREFIX.length());
            }
        }
        if (property.startsWith(datamartPrefix)) {
            Matcher matcher = propertyPattern.matcher(property);
            if (matcher.matches()) {
                return property.substring(datamartPrefix.length());
            }
        }
        return StringUtils.EMPTY;
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
