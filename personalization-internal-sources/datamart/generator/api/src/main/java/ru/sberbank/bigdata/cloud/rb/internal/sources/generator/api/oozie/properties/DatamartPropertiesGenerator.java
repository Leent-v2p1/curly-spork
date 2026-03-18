package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.SparkPropertiesBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileContent;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatamartPropertiesGenerator {

    private final PropertiesBuilderMapper propertiesBuilderMapper;

    public DatamartPropertiesGenerator(PropertiesBuilderMapper propertiesBuilderMapper) {
        this.propertiesBuilderMapper = propertiesBuilderMapper;
    }

    public List<FileContent> generate(List<Action> actionList) {
        return actionList.stream().flatMap(action -> {
            SparkPropertiesBuilder sparkPropertiesBuilder = propertiesBuilderMapper.getPropertiesBuilder(action);
            String sparkProperties = sparkPropertiesBuilder.buildSparkProperties();
            String propertiesName = action.name() + NameAdditions.PROPERTIES_POSTFIX;
            String sparkPropertiesName = PropertiesNameResolver.getSparkPropertiesTemplateName(propertiesName);
            FileContent sparkPropertiesFile = new FileContent(sparkPropertiesName, sparkProperties);

            String systemProperties = sparkPropertiesBuilder.buildSystemProperties();
            String systemPropertiesName = PropertiesNameResolver.getSystemPropertiesTemplateName(propertiesName);
            FileContent systemPropertiesFile = new FileContent(systemPropertiesName, systemProperties);

            return Stream.of(sparkPropertiesFile, systemPropertiesFile);
        }).collect(Collectors.toList());
    }
}
