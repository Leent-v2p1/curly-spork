package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config;

import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter.CustomSetter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter.ParamSetter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter.SparkDefaultSetter;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ParamSetterRunnerTest {

    @Test
    void run() {
        String initPropertyState = "init";
        ParamSetter defaultSetter = mock(SparkDefaultSetter.class);
        when(defaultSetter.run(eq(initPropertyState))).then(appendToArgument("_default"));
        ParamSetter customSetter = mock(CustomSetter.class);
        when(customSetter.run(eq("init_default"))).then(appendToArgument("_custom"));

        List<ParamSetter> paramSetters = Arrays.asList(defaultSetter, customSetter);
        ParamSetterRunner paramSetterRunner = new ParamSetterRunner();

        String result = paramSetterRunner.run(paramSetters, initPropertyState);

        assertEquals("init_default_custom", result);
    }

    private Answer<Object> appendToArgument(String custom) {
        return invocationOnMock -> {
            String inputProperties = invocationOnMock.getArgument(0);
            return inputProperties + custom;
        };
    }
}