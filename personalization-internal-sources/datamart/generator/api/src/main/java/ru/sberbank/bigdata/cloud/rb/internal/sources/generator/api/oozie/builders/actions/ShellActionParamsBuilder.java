package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.ExecutionShellSparkActionSetter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

public class ShellActionParamsBuilder extends AbstractActionParamsBuilder<ActionConf> {

    private final ExecutionShellSparkActionSetter actionSetter;

    public ShellActionParamsBuilder(String actionName, ActionConf actionConf, Environment env) {
        super(actionName, actionConf, env);
        this.actionSetter = new ExecutionShellSparkActionSetter(env);
    }

    public ShellActionParamsBuilder(String actionName,
                                    ActionConf actionConf,
                                    Environment env,
                                    ExecutionShellSparkActionSetter actionSetter) {
        super(actionName, actionConf, env);
        this.actionSetter = actionSetter;
    }

    @Override
    public ACTION buildActionParams() {
        final ACTION action = new ACTION();
        action.setName(WorkflowNameEscaper.escape(actionName));
        actionSetter.setAction(action, actionName);
        return action;
    }
}
