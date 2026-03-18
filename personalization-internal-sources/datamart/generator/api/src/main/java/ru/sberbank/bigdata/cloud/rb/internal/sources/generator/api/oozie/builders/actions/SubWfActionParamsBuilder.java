package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.FLAG;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.SUBWORKFLOW;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

public class SubWfActionParamsBuilder extends AbstractActionParamsBuilder<ActionConf> {
    private PathBuilder pathBuilder;

    public SubWfActionParamsBuilder(String actionName, ActionConf actionConf, Environment env) {
        super(actionName, actionConf, env);
        this.pathBuilder = new PathBuilder(env);
    }

    @Override
    public ACTION buildActionParams() {
        final ACTION action = new ACTION();
        action.setName(WorkflowNameEscaper.escape(actionName));

        final SUBWORKFLOW subworkflow = new SUBWORKFLOW();
        final FullTableName fullTableName = FullTableName.of(actionConf.getName());
        final String appPath = pathBuilder.resolveGeneratedWorkflow(fullTableName);
        subworkflow.setAppPath(appPath);
        subworkflow.setPropagateConfiguration(new FLAG());
        action.setSubWorkflow(subworkflow);

        return action;
    }
}
