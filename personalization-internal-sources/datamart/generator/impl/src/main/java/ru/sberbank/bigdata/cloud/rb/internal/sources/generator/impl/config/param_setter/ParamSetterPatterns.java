package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import java.util.regex.Pattern;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.ALLOW_FOR_ALL_PREFIX;

/**
 * CTL_PROPERTY - ищет $[имя_параметра]
 * (в имя_параметра допустимы ".", "_" и буквы)
 * CUSTOM_PARAM - ищет [ctlCustomConf]
 * CTL_EXECUTION - ищет [ctlExecutionConf]
 * SPARK_DEFAULTS - ищет [sparkAppDefaultConf]
 * ALL_TABLES_EXECUTION - ищет datamart.all.имя_параметра
 * (где имя_параметра это один из параметров executorMemory / executors / executorCoreNum / driverMemory)
 * ALL_TABLES_NONEXECUTION - ищет datamart.all.имя_параметра
 * (где имя_параметра любое кроме executorMemory / executors / executorCoreNum / driverMemory)
 */
public enum ParamSetterPatterns {
    CTL_PROPERTY(Pattern.compile("\\$\\[([\\d|\\w.]+)]")),
    CUSTOM_PARAM(Pattern.compile("\\[ctlCustomConf]")),
    CTL_EXECUTION(Pattern.compile("\\[ctlExecutionConf]")),
    SPARK_DEFAULTS(Pattern.compile("\\[sparkAppDefaultConf]")),
    ALL_TABLES_EXECUTION(Pattern.compile(ALLOW_FOR_ALL_PREFIX + ParamSetterPatterns.EXECUTION_PARAMS)),
    ALL_TABLES_NONEXECUTION(Pattern.compile(ALLOW_FOR_ALL_PREFIX + ParamSetterPatterns.NON_EXECUTION_PARAMS));

    // all acceptable execution params in regexp format
    public static final String EXECUTION_PARAMS = "(executorMemory|executors|executorCoreNum|driverMemory)";
    // all not acceptable params in regexp format, invert the #EXECUTION_PARAMS
    public static final String NON_EXECUTION_PARAMS = "(?!executorMemory|executors|executorCoreNum|driverMemory).*";

    final Pattern pattern;

    ParamSetterPatterns(Pattern pattern) {
        this.pattern = pattern;
    }

    public Pattern getPattern() {
        return pattern;
    }
}
