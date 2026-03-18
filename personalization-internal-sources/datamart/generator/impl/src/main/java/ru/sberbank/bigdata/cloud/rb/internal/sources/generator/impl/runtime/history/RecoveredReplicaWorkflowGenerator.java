package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.ActionTypeMapper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WfGenerator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.ActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.Action;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.beans.ActionImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.WorkflowAppXmlMarshaller;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_SCHEMA_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_TABLE_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.BUSINESS_DATE_STAT_ID;

/**
 * Генерирует поток для исторических витрин, если имелись пропуски в построении,
 * то добавляются экшины для построения 'реплики на дату' и последующего построения витрины для всех пропусков
 * и всегда добавляет экшин для построения витрина на базе реплики
 */
public class RecoveredReplicaWorkflowGenerator {

    private static final Logger log = LoggerFactory.getLogger(RecoveredReplicaWorkflowGenerator.class);

    private final FullTableName parentDatamartTableName;
    private final boolean isFirstLoading;
    private final LocalDate parentDatamartBuildDate;
    private final String parentDatamartAppPath;
    private final String replicaOnTheDateAppPath;
    private final Environment environment;
    private LocalDate currentDate;

    private RecoveredReplicaWorkflowGenerator(FullTableName parentDatamartTableName,
                                              boolean isFirstLoading,
                                              LocalDate parentDatamartBuildDate,
                                              String parentDatamartAppPath,
                                              String replicaOnTheDateAppPath,
                                              Environment environment) {
        this(parentDatamartTableName, isFirstLoading, parentDatamartBuildDate, parentDatamartAppPath, replicaOnTheDateAppPath, environment, LocalDate
                .now());
    }

    RecoveredReplicaWorkflowGenerator(FullTableName parentDatamartTableName,
                                      boolean isFirstLoading,
                                      LocalDate parentDatamartBuildDate,
                                      String parentDatamartAppPath,
                                      String replicaOnTheDateAppPath,
                                      Environment environment, LocalDate currentDate) {
        this.parentDatamartTableName = parentDatamartTableName;
        this.isFirstLoading = isFirstLoading;
        this.parentDatamartBuildDate = parentDatamartBuildDate;
        this.parentDatamartAppPath = parentDatamartAppPath;
        this.replicaOnTheDateAppPath = replicaOnTheDateAppPath;
        this.environment = environment;
        this.currentDate = currentDate;
    }

    public String generate() {
        List<Action> actions = new ArrayList<>();
        Map<String, RecoveryActionConf> confs = new HashMap<>();
        final List<LocalDate> dates = Stream
                .iterate(parentDatamartBuildDate, date -> date.plusDays(1L))
                .limit(Math.max(ChronoUnit.DAYS.between(parentDatamartBuildDate, currentDate), 0))
                .collect(toList());

        if (environment.isTestEnvironment()) {
            // test environment should always generate one iteration of replica recover and datamart build
            generateDayIteration(actions, confs, currentDate.minusDays(1L));
        } else {
            if (!isFirstLoading) {
                log.info("Generating recovery actions for dates = {}", dates);

                for (LocalDate date : dates) {
                    generateDayIteration(actions, confs, date);
                }
            }
        }

        final String regularDmName = "dm-on-the-date-" + currentDate;
        actions.add(new ActionImpl(regularDmName));
        confs.put(regularDmName, new RecoveryActionConf(regularDmName, parentDatamartAppPath, false, currentDate));

        final ActionTypeMapper mapper = new RecoveryActionTypeMapper(confs);
        final WORKFLOWAPP generate = new WfGenerator(mapper).generate(actions, "recovery-" + parentDatamartTableName.fullTableName());
        return new WorkflowAppXmlMarshaller().marshall(generate);
    }

    private void generateDayIteration(List<Action> actions, Map<String, RecoveryActionConf> confs, LocalDate date) {
        final String replicaOnTheDateName = "replica-on-the-date-" + date;
        actions.add(new ActionImpl(replicaOnTheDateName));
        final String dmOnTheDateName = "dm-on-the-date-" + date;
        actions.add(new ActionImpl(dmOnTheDateName));
        confs.put(replicaOnTheDateName, new RecoveryActionConf(replicaOnTheDateName, replicaOnTheDateAppPath, true, date));
        confs.put(dmOnTheDateName, new RecoveryActionConf(dmOnTheDateName, parentDatamartAppPath, true, date));
    }

    private static class RecoveryActionTypeMapper implements ActionTypeMapper {
        private final Map<String, RecoveryActionConf> confs;

        public RecoveryActionTypeMapper(Map<String, RecoveryActionConf> confs) {
            this.confs = confs;
        }

        @Override
        public ActionParamsBuilder resolve(String actionName) {
            return () -> {
                final ACTION action = new ACTION();
                action.setName(WorkflowNameEscaper.escape(actionName));

                final SUBWORKFLOW subworkflow = new SUBWORKFLOW();
                final RecoveryActionConf conf = confs.get(actionName);
                subworkflow.setAppPath(conf.appPath);
                if (conf.recoveryMode) {
                    subworkflow.setConfiguration(recoveryDateConf(conf));
                }
                subworkflow.setPropagateConfiguration(new FLAG());
                action.setSubWorkflow(subworkflow);

                return action;
            };
        }

        private CONFIGURATION recoveryDateConf(RecoveryActionConf conf) {
            final CONFIGURATION.Property property = new CONFIGURATION.Property();
            property.setName("recoveryDate");
            property.setValue(conf.onTheDate.toString());
            final CONFIGURATION configuration = new CONFIGURATION();
            configuration.getProperty().add(property);
            return configuration;
        }
    }


    private static class RecoveryActionConf {
        public final String replicaOnTheDateName;
        public final String appPath;
        public final boolean recoveryMode;
        public final LocalDate onTheDate;

        public RecoveryActionConf(String replicaOnTheDateName, String appPath, boolean recoveryMode, LocalDate onTheDate) {
            this.replicaOnTheDateName = replicaOnTheDateName;
            this.appPath = appPath;
            this.recoveryMode = recoveryMode;
            this.onTheDate = onTheDate;
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        final String resultSchema = SysPropertyTool.safeSystemProperty(RESULT_SCHEMA_PROPERTY);
        final String resultTable = SysPropertyTool.safeSystemProperty(RESULT_TABLE_PROPERTY);
        final FullTableName parentDatamartId = FullTableName.of(resultSchema, resultTable);
        final DatamartServiceFactory datamartServiceFactory = new DatamartServiceFactory(parentDatamartId);
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        final Integer parentDatamartTableNameEntityId = parametersService.ctlEntityId();
        final String profile = parametersService.ctlProfile();
        final LocalDate parentDatamartBuildDate = datamartServiceFactory.ctlRestApiCalls()
                .getStatisticAsLocalDate(parentDatamartTableNameEntityId, profile, BUSINESS_DATE_STAT_ID.getCode())
                .orElseThrow(() -> new IllegalStateException("must not be called when no ctlBuildDate is set for " + parentDatamartId));
        final Environment environment = parametersService.getEnv();

        final PathBuilder pathBuilder = new PathBuilder(environment);
        final String replicaOnTheDateAppPath = pathBuilder.resolveReplicaRecoveryDir(parentDatamartId);
        final String parentDatamartAppPath = pathBuilder.resolveGeneratedWorkflow(parentDatamartId);

        final RecoveredReplicaWorkflowGenerator recoveredReplicaWorkflowGenerator =
                new RecoveredReplicaWorkflowGenerator(
                        parentDatamartId,
                        parametersService.isFirstLoading(),
                        parentDatamartBuildDate,
                        parentDatamartAppPath,
                        replicaOnTheDateAppPath,
                        environment);

        final String generatedOozieWorkflow = recoveredReplicaWorkflowGenerator.generate();
        String resultAppPath = pathBuilder.resolveRuntimeWorkflowForRecovery(parentDatamartId);
        log.info("writing result xml in {}", resultAppPath);
        HDFSHelper.rewriteFile(generatedOozieWorkflow, resultAppPath);
    }
}
