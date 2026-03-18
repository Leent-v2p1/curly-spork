package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Class find all placeholders like $[param_name] in text
 * For each of them:
 * * if selected param name exists in #wfParam - it will be replaced on it
 * * if selected param name doesn't exists in #wfParam - it will be replaced on empty string and log the error
 */
public class CommonSetter extends ParamSetter {
    private final Logger log = LoggerFactory.getLogger(CommonSetter.class);
    private final Map<String, String> wfParam;

    public CommonSetter(Map<String, String> wfParam) {
        this.wfParam = wfParam;
    }

    @Override
    public String run(String property) {
        return replaceParam(property, wfParam, ParamSetterPatterns.CTL_PROPERTY.getPattern());
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
