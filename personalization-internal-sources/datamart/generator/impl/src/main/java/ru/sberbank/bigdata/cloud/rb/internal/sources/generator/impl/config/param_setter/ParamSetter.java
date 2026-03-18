package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.slf4j.Logger;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ParamSetter {

    /**
     * @param allProperty full config that need replacement
     * @param newProperty map of placeholder/value for every property should be replaced
     * @param pattern pattern of placeholders in allProperty
     *
     * @return new config with fully replaced placeholders to values from map
     */
    String replaceParam(String allProperty, Map<String, String> newProperty, Pattern pattern) {
        StringBuffer result = new StringBuffer();
        Matcher matcher = pattern.matcher(allProperty);
        while (matcher.find()) {
            String property = matcher.group(1);
            if (newProperty.containsKey(property)) {
                matcher.appendReplacement(result, newProperty.get(property));
            } else {
                matcher.appendReplacement(result, "");
                getLogger().warn("Parameter not found in ctl: {}", property);
            }
        }
        matcher.appendTail(result);
        return result.toString();
    }

    String replaceFirst(String allProperty, String newProperty, Pattern pattern) {
        Matcher matcher = pattern.matcher(allProperty);
        return matcher.replaceFirst(newProperty);
    }

    protected abstract Logger getLogger();

    public abstract String run(String property);
}
