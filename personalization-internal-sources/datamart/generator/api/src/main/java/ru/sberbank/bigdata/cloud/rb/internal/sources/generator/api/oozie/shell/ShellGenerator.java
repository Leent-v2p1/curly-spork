package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileContent;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShellGenerator {

    private final Map<String, ? extends ActionConf> properties;

    public ShellGenerator(Map<String, ? extends ActionConf> properties) {
        this.properties = properties;
    }

    public List<FileContent> generate(List<Action> actionList) {
        return actionList.stream().map(action -> {
            String actionName = action.name();
            final ActionConf actionConf = properties.get(actionName);
            SparkShellBuilder<? extends ActionConf> sparkShellBuilder = new SparkShellBuilder<>(actionName, actionConf);
            String shell = sparkShellBuilder.buildShell();
            String shellName = ShellNameResolver.getShellName(action.name());
            return new FileContent(shellName, shell);
        }).collect(Collectors.toList());
    }
}
