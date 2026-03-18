package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter.*;

import java.util.List;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

class ParamBuilderTest {

    @Test
    void addAllElements() {
        List<ParamSetter> result = ParamBuilder.builder()
                .withSparkDefault()
                .withCommon(emptyMap())
                .withExecute(emptyMap(), "")
                .withCustom(emptyMap(), "dm")
                .result();
        assertThat(result, hasSize(4));
        assertThat(result.get(0), is(instanceOf(SparkDefaultSetter.class)));
        assertThat(result.get(1), is(instanceOf(CommonSetter.class)));
        assertThat(result.get(2), is(instanceOf(ExecutionSetter.class)));
        assertThat(result.get(3), is(instanceOf(CustomSetter.class)));
    }

    @Test
    void someSettersNull() {
        List<ParamSetter> result = ParamBuilder.builder()
                .withSparkDefault()
                .withCustom(emptyMap(), "dm")
                .result();
        assertThat(result, hasSize(2));
        assertThat(result.get(0), is(instanceOf(SparkDefaultSetter.class)));
        assertThat(result.get(1), is(instanceOf(CustomSetter.class)));
    }

    @Test
    void wrongOrder() {
        List<ParamSetter> result = ParamBuilder.builder()
                .withCustom(emptyMap(), "dm")
                .withSparkDefault()
                .result();
        assertThat(result, hasSize(2));
        assertThat(result.get(0), is(instanceOf(SparkDefaultSetter.class)));
        assertThat(result.get(1), is(instanceOf(CustomSetter.class)));
    }
}