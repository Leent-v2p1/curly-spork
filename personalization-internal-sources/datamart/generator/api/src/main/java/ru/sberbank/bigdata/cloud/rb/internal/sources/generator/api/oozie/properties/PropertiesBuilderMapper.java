package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.SparkPropertiesBuilder;

public interface PropertiesBuilderMapper {

    SparkPropertiesBuilder<? extends ActionConf> getPropertiesBuilder(Action action);
}
