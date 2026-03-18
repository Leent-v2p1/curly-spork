package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter.ParamSetter;

import java.util.List;

public class ParamSetterRunner {
    private static final Logger log = LoggerFactory.getLogger(ParamSetterRunner.class);

    public String run(List<ParamSetter> paramSetterList, String initPropertyState) {
        String property = initPropertyState;
        for (ParamSetter paramSetter : paramSetterList) {
            property = paramSetter.run(property);
            log.info("Updated property with {}, result is {}", paramSetter, property);
        }
        return property;
    }
}
