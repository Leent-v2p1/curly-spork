package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.SparkPropertiesBuilder;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DatamartPropertiesGeneratorTest {

    @Test
    void testGenerate() {
        PropertiesBuilderMapper builderMapper = mock(PropertiesBuilderMapper.class);
        SparkPropertiesBuilder propertiesBuilder = mock(SparkPropertiesBuilder.class);
        when(builderMapper.getPropertiesBuilder(any())).thenReturn(propertiesBuilder);

        new DatamartPropertiesGenerator(builderMapper).generate(Arrays.asList(new ActionImpl("action1"),
                new ActionImpl("action2")));

        verify(propertiesBuilder, times(2)).buildSparkProperties();
        verify(propertiesBuilder, times(2)).buildSystemProperties();
    }
}
