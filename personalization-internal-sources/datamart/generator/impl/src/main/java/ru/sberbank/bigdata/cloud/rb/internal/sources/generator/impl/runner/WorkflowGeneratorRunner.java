package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runner;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.PropertiesUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.CtlStatisticService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.data_quality_check.DataQualityCheckService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.kafka.KafkaSaverService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.repartition.RepartitionService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.ReservingService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.TargetPathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WfResourcesGenerator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowWithResources;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConfReader;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ResultActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.uat.ActionDependency;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.properties.PropertiesBuilderMapperImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.WorkflowWriter;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonFiles.PERSONALIZATION_JAR;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.CTL_STATS_PUBLISHER_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.RESERVE_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.*;

public class WorkflowGeneratorRunner {

    private final Environment env;
    private final TargetPathBuilder pathBuilder;
    private final WorkflowWriter workflowWriter;
    private final FileWriter fileWriter;

    public WorkflowGeneratorRunner(Environment env) {
        this.env = env;
        this.pathBuilder = new TargetPathBuilder(env);
        this.workflowWriter = new WorkflowWriter();
        this.fileWriter = new FileWriter();
    }

    WorkflowGeneratorRunner(Environment env,
                            TargetPathBuilder pathBuilder,
                            WorkflowWriter workflowWriter,
                            FileWriter fileWriter) {
        this.env = env;
        this.pathBuilder = pathBuilder;
        this.workflowWriter = workflowWriter;
        this.fileWriter = fileWriter;
    }

    public void generateAllWorkflowXml() {
        // action для обычных витрин. порядок исполнения такой:
        // properties -> datamart -> properties -> dqcheck -> properties -> repartitioner -> properties -> kafka -> properties -> reserving -> properties -> ctlStatsPublisher
        List<ResultActionConf> datamartConfList = createConfList(WorkflowType.DATAMART);
        List<ResultActionConf> repartitionerConfList = createRepartitioner(datamartConfList);
        List<ResultActionConf> dataQualityCheckList = createDataQualityCheck(datamartConfList);
        List<ResultActionConf> kafkaSaverList = createKafkaSaver(datamartConfList);
        List<ResultActionConf> reservingConfList = createReserving(datamartConfList);
        List<ResultActionConf> ctlStatsPublisherList = createCtlStatsPublisher(datamartConfList);
        //для особых типов витрин
        List<ResultActionConf> stageConfList = createConfList(WorkflowType.STAGE);
        List<ResultActionConf> helperConfList = createConfList(WorkflowType.HELPER);

        List<ResultActionConf> allConfList = new ArrayList<>();
        allConfList.addAll(datamartConfList);
        allConfList.addAll(dataQualityCheckList);
        allConfList.addAll(repartitionerConfList);
        allConfList.addAll(kafkaSaverList);
        allConfList.addAll(reservingConfList);
        allConfList.addAll(ctlStatsPublisherList);
        allConfList.addAll(stageConfList);
        allConfList.addAll(helperConfList);

        List<ResultActionConf> propertiesServiceConfList = createPropertiesService(allConfList);
        allConfList.addAll(propertiesServiceConfList);

        //lastAction содержит список корневых сущностей из списка зависимостей которой будет определяться граф построения oozie потока
        List<ResultActionConf> lastAction = new ArrayList<>();
        lastAction.addAll(ctlStatsPublisherList);//для обычных витрин, последний action всегда это публикация статистик
        lastAction.addAll(helperConfList);

        generateAndWriteWorkflows(allConfList, lastAction);
    }

    private void generateAndWriteWorkflows(List<ResultActionConf> allConfList, List<ResultActionConf> startConfList) {
        Map<String, ActionConf> actionConfMap = new ActionConfReader().asMap(allConfList);
        PropertiesBuilderMapperImpl propertiesBuilderMapper = new PropertiesBuilderMapperImpl(actionConfMap, env);

        WfResourcesGenerator resourcesGenerator = new WfResourcesGenerator(actionConfMap, propertiesBuilderMapper,
                env, actionConfMap);

        ActionDependency actionDependency = new ActionDependency(allConfList);

        startConfList.forEach(datamartConf -> {
            String workflowName = datamartConf.getWorkflowName();
            Path path = pathBuilder.resolveGeneratedWorkflowBuildDir(FullTableName.of(workflowName));
            List<Action> actionList = actionDependency.resolve(datamartConf);
            WorkflowWithResources resources = resourcesGenerator.generate(actionList, workflowName);

            workflowWriter.writeWorkflow(path, resources.workflow);
            fileWriter.write(path, resources.fileContentList);
        });
    }

    private List<ResultActionConf> createConfList(WorkflowType type) {
        Set<String> actionIdList = propertiesUtilWrapper(type);
        return actionIdList.stream()
                .map(this::getActionConf)
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    Set<String> propertiesUtilWrapper(WorkflowType type) {
        return PropertiesUtils.getAllActionsIdWithType(type);
    }

    private ResultActionConf getActionConf(String datamart) {
        DatamartProperties properties = new DatamartProperties(FullTableName.of(datamart));
        ResultActionConf actionConf = new ResultActionConf();
        final List<String> extraJarNames = Stream.of(properties.getJarName(), PERSONALIZATION_JAR).distinct().collect(Collectors.toList());
        actionConf.setName(datamart);
        actionConf.setJarName(properties.getJarName());
        actionConf.setExtraJarNames(extraJarNames);
        actionConf.setWorkflowName(datamart);
        actionConf.setStart(false);
        actionConf.setType(properties.getWorkflowType().toString().toLowerCase());
        actionConf.setSourceSchema(properties.getSourceSchema());
        actionConf.setClassName(properties.getClassName());
        actionConf.setMemoryPreset(properties.getMemoryPreset());
        actionConf.setDriverMemory(properties.getDriverMemory());
        actionConf.setExecutorMemory(properties.getExecutorMemory());
        actionConf.setExecutorCoreNum(properties.getExecutorCoreNum());
        actionConf.setExecutors(properties.getExecutors());
        actionConf.setDriverMemoryOverhead(properties.getDriverMemoryOverhead());
        actionConf.setExecutorMemoryOverhead(properties.getExecutorMemoryOverhead());
        actionConf.setSparkSqlShufflePartitions(properties.getSparkSqlShufflePartitions());
        actionConf.setDependencies(properties.getDependencies());
        actionConf.setCustomJavaOpts(properties.customJavaOpts());
        return actionConf;
    }

    @VisibleForTesting
    List<ResultActionConf> createReserving(List<ResultActionConf> actionConfs) {
        return actionConfs.stream()
                .filter(actionConf -> actionConf.getType().equals(WorkflowType.DATAMART.getKey()))
                .map(datamartConf -> {
                    ResultActionConf resultedConf = new ResultActionConf();
                    List<String> extraJarNames = Stream.of(datamartConf.getJarName(), PERSONALIZATION_JAR).distinct().collect(Collectors.toList());
                    resultedConf.setName(datamartConf.getName() + NameAdditions.RESERVE_POSTFIX);
                    resultedConf.setJarName(PERSONALIZATION_JAR);
                    resultedConf.setExtraJarNames(extraJarNames);
                    resultedConf.setWorkflowName(datamartConf.getName());
                    resultedConf.setType(WorkflowType.RESERVING.getKey());
                    resultedConf.setStart(false);
                    final boolean repartitionEnabled = ENABLED_FLAG.equals(datamartConf.getCustomJavaOpts()
                            .get(REPARTITION_PROPERTY));
                    final boolean dqEnabled = ENABLED_FLAG.equals(datamartConf.getCustomJavaOpts()
                            .get(DQCHECK_PROPERTY));
                    final boolean kafkaEnabled = ENABLED_FLAG.equals(datamartConf.getCustomJavaOpts()
                            .get(KAFKA_PROPERTY));
                    final String dqStep = datamartConf.getName() + NameAdditions.DATA_QUALITY_CHECK_POSTFIX;
                    final String repartitionStep = datamartConf.getName() + NameAdditions.REPARTITIONER_POSTFIX;
                    final String kafkaStep = datamartConf.getName() + NameAdditions.KAFKA_SAVER_POSTFIX;
                    final String datamartStep = datamartConf.getName();
                    String previousStep = repartitionEnabled || dqEnabled || kafkaEnabled
                            ? (kafkaEnabled ? kafkaStep
                            : (repartitionEnabled ? repartitionStep : dqStep))
                            : datamartStep;
                    resultedConf.setDependencies(Collections.singletonList(previousStep));
                    resultedConf.setClassName(ReservingService.class.getName());
                    return resultedConf;
                })
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    List<ResultActionConf> createPropertiesService(List<ResultActionConf> actionConfs) {
        return actionConfs.stream()
                .filter(actionConf -> !actionConf.getType().equals(WorkflowType.SUB_WORKFLOW.getKey()))
                .map(datamartConf -> {
                    ResultActionConf resultedConf = new ResultActionConf();
                    resultedConf.setName(datamartConf.getName() + NameAdditions.PROPERTIES_POSTFIX);
                    resultedConf.setJarName(PERSONALIZATION_JAR);
                    resultedConf.setExtraJarNames(Collections.singletonList(PERSONALIZATION_JAR));
                    resultedConf.setWorkflowName(datamartConf.getName());
                    resultedConf.setType(WorkflowType.PROPERTIES.getKey());
                    resultedConf.setStart(false);
                    //Чтобы вставить propertiesAction перед datamartAction, берём dependencies из Datamart,
                    // а в datamartService проставляем propertiesAction в качестве dependencies
                    resultedConf.setDependencies(datamartConf.getDependencies());
                    datamartConf.setDependencies(Collections.singletonList(resultedConf.getName()));
                    resultedConf.setClassName(DatamartProperties.class.getName());
                    return resultedConf;
                })
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    List<ResultActionConf> createRepartitioner(List<ResultActionConf> actionConfs) {
        return actionConfs.stream()
                .filter(actionConf -> actionConf.getType().equals(WorkflowType.DATAMART.getKey()))
                .filter(actionConf -> ENABLED_FLAG.equals(actionConf.getCustomJavaOpts().get(REPARTITION_PROPERTY)))
                .map(datamartConf -> {
                    ResultActionConf resultedConf = new ResultActionConf();
                    resultedConf.setName(datamartConf.getName() + NameAdditions.REPARTITIONER_POSTFIX);
                    resultedConf.setJarName(PERSONALIZATION_JAR);
                    resultedConf.setExtraJarNames(Collections.singletonList(PERSONALIZATION_JAR));
                    resultedConf.setWorkflowName(datamartConf.getName());
                    resultedConf.setType(WorkflowType.REPARTITIONER.getKey());
                    resultedConf.setStart(false);
                    final boolean dqEnabled = ENABLED_FLAG.equals(datamartConf.getCustomJavaOpts()
                            .get(DQCHECK_PROPERTY));
                    final String dqStep = datamartConf.getName() + NameAdditions.DATA_QUALITY_CHECK_POSTFIX;
                    final String datamartStep = datamartConf.getName();
                    String previousStep = dqEnabled ? dqStep : datamartStep;
                    resultedConf.setDependencies(Collections.singletonList(previousStep));
                    resultedConf.setClassName(RepartitionService.class.getName());
                    return resultedConf;
                })
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    List<ResultActionConf> createCtlStatsPublisher(List<ResultActionConf> actionConfs) {
        return actionConfs.stream()
                .filter(actionConf -> actionConf.getType().equals(WorkflowType.DATAMART.getKey()))
                .map(datamartConf -> {
                    ResultActionConf resultedConf = new ResultActionConf();
                    resultedConf.setName(datamartConf.getName() + CTL_STATS_PUBLISHER_POSTFIX);
                    resultedConf.setJarName(PERSONALIZATION_JAR);
                    resultedConf.setExtraJarNames(Collections.singletonList(PERSONALIZATION_JAR));
                    resultedConf.setWorkflowName(datamartConf.getName());
                    resultedConf.setType(WorkflowType.CTL_STATS_PUBLISHER.getKey());
                    resultedConf.setStart(false);
                    resultedConf.setDependencies(Collections.singletonList(datamartConf.getName() + RESERVE_POSTFIX));
                    resultedConf.setClassName(CtlStatisticService.class.getName());
                    return resultedConf;
                })
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    List<ResultActionConf> createDataQualityCheck(List<ResultActionConf> actionConfs) {
        return actionConfs.stream()
                .filter(actionConf -> actionConf.getType().equals(WorkflowType.DATAMART.getKey()))
                .filter(actionConf -> ENABLED_FLAG.equals(actionConf.getCustomJavaOpts().get(DQCHECK_PROPERTY)))
                .map(datamartConf -> {
                    ResultActionConf resultedConf = new ResultActionConf();
                    List<String> extraJarNames = Stream.of(datamartConf.getJarName(), PERSONALIZATION_JAR).distinct().collect(Collectors.toList());
                    resultedConf.setName(datamartConf.getName() + NameAdditions.DATA_QUALITY_CHECK_POSTFIX);
                    resultedConf.setJarName(PERSONALIZATION_JAR);
                    resultedConf.setExtraJarNames(extraJarNames);
                    resultedConf.setWorkflowName(datamartConf.getName());
                    resultedConf.setType(WorkflowType.DQCHECK.getKey());
                    resultedConf.setStart(false);
                    resultedConf.setDependencies(Collections.singletonList(datamartConf.getName()));
                    resultedConf.setClassName(DataQualityCheckService.class.getName());
                    return resultedConf;
                })
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    List<ResultActionConf> createKafkaSaver(List<ResultActionConf> actionConfs) {
        return actionConfs.stream()
                .filter(actionConf -> actionConf.getType().equals(WorkflowType.DATAMART.getKey()))
                .filter(actionConf -> ENABLED_FLAG.equals(actionConf.getCustomJavaOpts().get(KAFKA_PROPERTY)))
                .map(datamartConf -> {
                    ResultActionConf resultedConf = new ResultActionConf();
                    List<String> extraJarNames = Stream.of(PERSONALIZATION_JAR).collect(Collectors.toList());
                    resultedConf.setName(datamartConf.getName() + NameAdditions.KAFKA_SAVER_POSTFIX);
                    resultedConf.setJarName(PERSONALIZATION_JAR);
                    resultedConf.setExtraJarNames(extraJarNames);
                    resultedConf.setWorkflowName(datamartConf.getName());
                    resultedConf.setType(WorkflowType.KAFKA.getKey());
                    resultedConf.setStart(false);
                    final boolean repartitionEnabled = ENABLED_FLAG.equals(datamartConf.getCustomJavaOpts()
                            .get(REPARTITION_PROPERTY));
                    final boolean dqEnabled = ENABLED_FLAG.equals(datamartConf.getCustomJavaOpts()
                            .get(DQCHECK_PROPERTY));
                    final String dqStep = datamartConf.getName() + NameAdditions.DATA_QUALITY_CHECK_POSTFIX;
                    final String repartitionStep = datamartConf.getName() + NameAdditions.REPARTITIONER_POSTFIX;
                    final String datamartStep = datamartConf.getName();
                    String previousStep = repartitionEnabled || dqEnabled
                            ? (repartitionEnabled ? repartitionStep : dqStep)
                            : datamartStep;
                    resultedConf.setDependencies(Collections.singletonList(previousStep));
                    resultedConf.setClassName(KafkaSaverService.class.getName());
                    return resultedConf;
                })
                .collect(Collectors.toList());
    }
}
