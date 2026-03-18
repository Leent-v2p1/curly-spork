package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.PropertiesShellSparkActionSetter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

public class PropertiesServiceActionParamsBuilder extends AbstractActionParamsBuilder<ActionConf> {

    private final PropertiesShellSparkActionSetter actionSetter;

    public PropertiesServiceActionParamsBuilder(String actionName, ActionConf actionConf, Environment env) {
        super(actionName, actionConf, env);
        this.actionSetter = new PropertiesShellSparkActionSetter(env);
    }

    public PropertiesServiceActionParamsBuilder(String actionName,
                                                ActionConf actionConf,
                                                Environment env,
                                                PropertiesShellSparkActionSetter actionSetter) {
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
