package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.DatamartPropertiesGenerator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.PropertiesBuilderMapper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.ShellGenerator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileContent;

import java.util.List;
import java.util.Map;

public class WfResourcesGenerator {

    private final WfGenerator generator;
    private final ShellGenerator shellGenerator;
    private final DatamartPropertiesGenerator propertiesGenerator;
    private final SparkActionFilter sparkActionFilter;

    public WfResourcesGenerator(WfGenerator generator,
                                ShellGenerator shellGenerator,
                                DatamartPropertiesGenerator propertiesGenerator,
                                SparkActionFilter sparkActionFilter) {
        this.generator = generator;
        this.shellGenerator = shellGenerator;
        this.propertiesGenerator = propertiesGenerator;
        this.sparkActionFilter = sparkActionFilter;
    }

    public WfResourcesGenerator(Map<String, ? extends ActionConf> properties,
                                PropertiesBuilderMapper propertiesBuilderMapper,
                                Environment env,
                                Map<String, ? extends ActionConf> actionConfMap) {
        ActionTypeMapper actionTypeMapper = new ActionTypeMapperImpl(actionConfMap, env);
        this.generator = new WfGenerator(actionTypeMapper);
        this.shellGenerator = new ShellGenerator(properties);
        this.propertiesGenerator = new DatamartPropertiesGenerator(propertiesBuilderMapper);
        this.sparkActionFilter = new SparkActionFilter(actionConfMap);
    }

    public WorkflowWithResources generate(List<Action> actionList, String workflowName) {
        List<Action> sparkActionList = sparkActionFilter.get(actionList);
        List<FileContent> fileContentList = shellGenerator.generate(sparkActionList);
        fileContentList.addAll(propertiesGenerator.generate(sparkActionList));

        WORKFLOWAPP app = generator.generate(actionList, workflowName);

        return new WorkflowWithResources(fileContentList, app);
    }
}
