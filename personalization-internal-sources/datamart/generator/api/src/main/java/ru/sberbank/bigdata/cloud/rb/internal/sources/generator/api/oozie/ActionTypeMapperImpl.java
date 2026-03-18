package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.ActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.PropertiesServiceActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.ShellActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.SubWfActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.Map;

public class ActionTypeMapperImpl implements ActionTypeMapper {
    private final Map<String, ? extends ActionConf> properties;
    private final Environment env;

    public ActionTypeMapperImpl(Map<String, ? extends ActionConf> properties, Environment env) {
        this.properties = properties;
        this.env = env;
    }

    @Override
    public ActionParamsBuilder resolve(String actionName) {
        final ActionConf actionConf = properties.get(actionName);
        final String type = actionConf.getType();
        final WorkflowType workflowType = WorkflowType.valueOfByKey(type);

        System.out.println(workflowType.toString());
        switch (workflowType) {
            case SUB_WORKFLOW:
                return new SubWfActionParamsBuilder(actionName, actionConf, env);
            case DATAMART:
            case STAGE:
            case RESERVING:
            case REPARTITIONER:
            case DQCHECK:
            case KAFKA:
            case CTL_STATS_PUBLISHER:
            case HELPER:
                return new ShellActionParamsBuilder(actionName, actionConf, env);
            case PROPERTIES:
                return new PropertiesServiceActionParamsBuilder(actionName, actionConf, env);
            default:
                throw new UnsupportedOperationException(workflowType.toString());
        }
    }
}
