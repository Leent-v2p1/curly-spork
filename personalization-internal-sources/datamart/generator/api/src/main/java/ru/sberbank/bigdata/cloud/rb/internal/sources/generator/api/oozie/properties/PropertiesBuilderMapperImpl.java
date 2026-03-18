package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.builder.*;

import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.StringUtils.removePostfix;

public class PropertiesBuilderMapperImpl implements PropertiesBuilderMapper {

    private final Map<String, ActionConf> properties;
    private final Environment env;

    public PropertiesBuilderMapperImpl(Map<String, ActionConf> properties, Environment env) {
        this.properties = properties;
        this.env = env;
    }

    public SparkPropertiesBuilder<? extends ActionConf> getPropertiesBuilder(Action action) {
        String actionName = action.name();
        final ActionConf actionConf = properties.get(actionName);
        WorkflowType workflowType = WorkflowType.valueOfByKey(actionConf.getType());

        switch (workflowType) {
            case DATAMART:
                return new SparkPropertiesBuilder<>(actionName, actionConf, env, getProperties(actionName));
            case STAGE:
                return new SparkStagePropertiesBuilder(actionName, actionConf, env, getProperties(actionName));
            case RESERVING:
                final String reservingName = removePostfix(actionName, NameAdditions.RESERVE_POSTFIX);
                return new SparkReservingPropertiesBuilder(actionName, actionConf, env, getProperties(reservingName));
            case CTL_STATS_PUBLISHER:
                final String publisherName = removePostfix(actionName, NameAdditions.CTL_STATS_PUBLISHER_POSTFIX);
                return new CtlStatisticPublisherPropertiesBuilder(actionName, actionConf, env, getProperties(publisherName));
            case REPARTITIONER:
                return createSparkRepartitionerPropertiesBuilder(actionName, actionConf);
            case DQCHECK:
                return createSparkDataQualityCheckPropertiesBuilder(actionName, actionConf);
            case KAFKA:
                return createSparkKafkaSaverPropertiesBuilder(actionName, actionConf);
            case HELPER:
                return new SparkHelperPropertiesBuilder(actionName, actionConf, env, getProperties(actionName));
            default:
                throw new UnsupportedOperationException(workflowType.toString());
        }
    }

    public DatamartProperties getProperties(String actionName) {
        return new DatamartProperties(FullTableName.of(actionName));
    }

    private SparkPropertiesBuilder<? extends ActionConf> createSparkRepartitionerPropertiesBuilder(String actionName,
                                                                                                   ActionConf actionConf) {
        final String repartitionerActionName = removePostfix(actionName, NameAdditions.REPARTITIONER_POSTFIX);
        final ActionConf datamartActionConf = properties.get(repartitionerActionName);
        return new SparkRepartitionerPropertiesBuilder(actionName, actionConf, datamartActionConf, env, getProperties(repartitionerActionName));
    }

    private SparkPropertiesBuilder<? extends ActionConf> createSparkDataQualityCheckPropertiesBuilder(String actionName,
                                                                                                   ActionConf actionConf) {
        final String dqcheckActionName = removePostfix(actionName, NameAdditions.DATA_QUALITY_CHECK_POSTFIX);
        final ActionConf datamartActionConf = properties.get(dqcheckActionName);
        return new SparkDataQualityCheckPropertiesBuilder(actionName, actionConf, datamartActionConf, env, getProperties(dqcheckActionName));
    }

    private SparkPropertiesBuilder<? extends ActionConf> createSparkKafkaSaverPropertiesBuilder(String actionName,
                                                                                                      ActionConf actionConf) {
        final String kafkaSaverActionName = removePostfix(actionName, NameAdditions.KAFKA_SAVER_POSTFIX);
        final ActionConf datamartActionConf = properties.get(kafkaSaverActionName);
        return new SparkDataQualityCheckPropertiesBuilder(actionName, actionConf, datamartActionConf, env, getProperties(kafkaSaverActionName));
    }
}
