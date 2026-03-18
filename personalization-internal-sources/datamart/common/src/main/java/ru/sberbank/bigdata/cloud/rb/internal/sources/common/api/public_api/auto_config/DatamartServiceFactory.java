package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config;

import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.GraphiteSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.ExtraStatisticAccumulator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.Parametrizer;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.StringReplacer;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.KeyChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.HiveDatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.ReplicaNameResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.DatamartSaver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TempSaver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemoverHandler;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.EpkAppenderService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.MappingService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.OverwriteSavingStartegy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlDefaultStatistics;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticMerger;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticPublisherService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticToFileWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV5;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.ListenerType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.MetricProviderObserver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.TaskInfoRecorderListener;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.TaskMetricsService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.diagnostic.graphite.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.StatTableWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodDatamartsMerger;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.EribDatamartsMerger;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.DEFAULT_VALUE_CTL_VERSION_V5;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOADED_STAT_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.getSystemProperty;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

/**
 * Class contains methods to create instances of different services for datamart such as DatamartNaming, PropertiesService
 */
public class DatamartServiceFactory extends ServiceFactory {

    private static final Logger log = LoggerFactory.getLogger(DatamartServiceFactory.class);

    private final FullTableName datamartId;
    private final Environment env;
    //singleton instances
    private TaskMetricsService metricsService;
    private DatamartNaming naming;
    private GraphiteListenerCreator graphiteListenerCreator;
    private CtlApiCalls ctlApiCalls;
    private ParametersService parametersService;

    public DatamartServiceFactory(FullTableName datamartId) {
        this(datamartId, datamartId.fullTableName());
    }

    public DatamartServiceFactory(FullTableName datamartId, String jobName) {
        this(datamartId, jobName, Collections.emptyMap(), Collections.emptyMap());
    }

    public DatamartServiceFactory(FullTableName datamartId,
                                  String jobName,
                                  Map<String, String> sparkContextProperties,
                                  Map<String, String> sparkContextLocalProperties) {
        super(jobName, sparkContextProperties, sparkContextLocalProperties);
        this.datamartId = datamartId;
        this.env = Environment.valueOf(getSystemProperty("spark.environment", "production").toUpperCase());
    }

    /**
     * @return instance of service with parameters(some parameters could be result of calculation from different sources) for datamart
     */
    public ParametersService parametersService() {
        if (parametersService == null) {
            ToDeleteResolver toDeleteResolver = new ToDeleteResolver();
            this.parametersService = ParameterServiceBuilder.builder()
                    .setDatamartId(datamartId)
                    .setEnv(env)
                    .setToDeleteResolver(toDeleteResolver)
                    .setCtlRestApi(ctlRestApiCalls())
                    .setSparkSession(sqlContext())
                    .build();
        }
        return parametersService;
    }

    /**
     * @return instance of service for client_epk appender
     */
    public EpkAppenderService epkIdAppender() {
        return new EpkAppenderService(datamartContext(), parametersService().sourcePostfix());
    }

    /**
     * @return instance of service for MappingService, that generates temp tables with mapping of client's ids
     */
    public MappingService idMappingService() {
        return new MappingService(datamartContext());
    }

    public StatisticPublisherService statisticPublisherService() {
        ParametersService ps = parametersService();
        return new StatisticPublisherService(
                ctlRestApiCalls(),
                ps.ctlEntityIdOptional(),
                ps.getEnv(),
                ps.ctlProfile()
        );
    }

    public CtlApiCalls ctlRestApiCalls() {
        if (ctlApiCalls == null) {
            log.info("Creating instance of CtlApiCalls");
            String ctlUrl = safeSystemProperty("spark.ctl.url");
            boolean ctlApiVersionV5 = Boolean.parseBoolean(getSystemProperty("spark.ctl.api.version.v5", DEFAULT_VALUE_CTL_VERSION_V5));
            if (ctlApiVersionV5){
                this.ctlApiCalls = new CtlApiCallsV5(ctlUrl);
            } else {
                this.ctlApiCalls = new CtlApiCallsV1(ctlUrl);
            }

        }
        return ctlApiCalls;
    }

    public PathBuilder pathBuilder() {
        return new PathBuilder(env);
    }

    public DatamartContext datamartContext() {
        return HiveDatamartContext.builder(sqlContext())
                .naming(naming())
                .environment(parametersService().getEnv())
                .grantsChecker(schemaGrantsChecker())
                .parametrizer(parametrizer())
                .create();
    }

    public BuildRequiredChecker buildRequiredChecker() {
        return new BuildRequiredChecker(
                parametersService().getLastStatistic(LAST_LOADED_STAT_ID),
                parametersService().skipIfBuiltToday()
        );
    }

    public EmergencyStopRequiredChecker emergencyStopRequiredChecker() {
        return new EmergencyStopRequiredChecker(
                parametersService().emergencyStopRequired()
        );
    }

    public KafkaSaverRequiredChecker kafkaSaverRequiredChecker() {
        return new KafkaSaverRequiredChecker(
                parametersService().kafkaSaverRequired()
        );
    }

    public DatamartSaver datamartSaver() {
        return datamartSaver(new OverwriteSavingStartegy());
    }

    public DatamartSaver datamartSaver(HiveSavingStrategy savingStrategy) {
        return new DatamartSaver(sqlContext(), naming(), savingStrategy, schemaGrantsChecker(), parametrizer());
    }

    public TempSaver tempSaver() {
        return new TempSaver(sqlContext(), naming(), parametrizer());
    }

    public DatamartNaming naming() {
        if (naming == null) {
            naming = new DatamartNaming(
                    parametersService().getEnv(),
                    parametersService().sourceSchema(),
                    parametersService().targetSchema(),
                    parametersService().targetTable(),
                    parametersService().sourcePostfix()
            );
        }
        return naming;
    }

    public DatamartRunner datamartRunner(GraphiteSender sender) {
        final ScheduledReporter reporter = graphiteListenerCreator(sender).metricsReporter();
        final GraphiteListener graphiteListener = new GraphiteListener(reporter);
        String statisticTempFile = pathBuilder().resolveStatisticTempFile(datamartId, parametersService().ctlLoadingId());
        final DatamartRunner datamartRunner = new DatamartRunner(metricsService(sender), buildRequiredChecker(), statisticTempFile);
        datamartRunner.subscribe(graphiteListener);
        return datamartRunner;
    }

    @VisibleForTesting
    TaskMetricsService metricsService(GraphiteSender sender) {
        if (this.metricsService == null) {
            final GraphiteListenerCreator listenerCreator = graphiteListenerCreator(sender);
            Environment environment = parametersService().getEnv();
            final ExecutorAllocationListener executorsMetricProvider = listenerCreator.createExecutorsMetricProvider(environment);
            final CurrentTasksListener taskNumMetricProvider = listenerCreator.createTaskMetricProvider(environment);

            TaskInfoRecorderListener.instance().subscribe(ListenerType.EXECUTOR, executorsMetricProvider);
            TaskInfoRecorderListener.instance().subscribe(ListenerType.TASK, taskNumMetricProvider);

            MetricProviderObserver metricProviderObserver = new MetricProviderObserver();
            final GraphiteMetricsProvider datamartMetricProvider = listenerCreator.createDatamartMetricProvider(environment);
            metricProviderObserver.addProvider(datamartMetricProvider);
            this.metricsService = new TaskMetricsService(
                    metricProviderObserver,
                    sqlContext(),
                    datamartId,
                    naming(),
                    parametersService().workflowType(),
                    TaskInfoRecorderListener.instance(),
                    parametersService().ctlLoadingId(),
                    parametersService().ctlEntityIdOptional().orElse(-1),
                    parametersService().ctlBuildDate(),
                    parametersService().isFirstLoading()
            );
        }
        return metricsService;
    }

    @VisibleForTesting
    GraphiteListenerCreator graphiteListenerCreator(GraphiteSender sender) {
        if (this.graphiteListenerCreator == null) {
            log.info("created GraphiteListenerCreator");
            this.graphiteListenerCreator = new GraphiteListenerCreator(naming(), sender);
        }
        return this.graphiteListenerCreator;
    }

    public CodDatamartsMerger codDatamartsMerger() {
        return new CodDatamartsMerger(datamartContext());
    }

    public EribDatamartsMerger eribDatamartsMerger() {
        return new EribDatamartsMerger(datamartContext());
    }

    public DatamartAutoConfigurer datamartAutoConfigurer() {
        return new DatamartAutoConfigurer(this);
    }

    public KeyChecker keyChecker() {
        return new KeyChecker(tempSaver(), naming());
    }

    public StringReplacer emptyStringReplacer() {
        return new StringReplacer("", null);
    }

    public ExtraStatisticAccumulator extraStatisticAccumulator() {
        return new ExtraStatisticAccumulator();
    }

    public ExtraStatisticAccumulator disabledStatisticAccumulator() {
        return new ExtraStatisticAccumulator();
    }

    public StatisticToFileWriter statisticToFileWriter() {
        final PathBuilder pathBuilder = new PathBuilder(env);
        String pathToStatistic = pathBuilder.resolveStatisticTempFile(datamartId, parametersService().ctlLoadingId());
        String pathToDisabledStatistic = pathBuilder.resolveDisabledStatisticTempFile(datamartId, parametersService().ctlLoadingId());
        return new StatisticToFileWriter(pathToStatistic, pathToDisabledStatistic);
    }

    public CtlDefaultStatistics ctlDefaultStatistics() {
        return new CtlDefaultStatistics(
                parametersService().shouldUpdateMonthStat(),
                parametersService().ctlBuildDate(),
                LocalDateTime.now(),
                parametersService().ctlLoadingId(),
                parametersService().loadingTypeStat()
        );
    }

    public StatTableWriter statTableWriter() {
        return new StatTableWriter(
                datamartContext(),
                sqlContext(),
                parametersService().ctlLoadingId(),
                parametersService().ctlProfile(),
                parametersService().targetSchema(),
                parametersService().targetTable(),
                parametersService().ctlEntityId(),
                Timestamp.from(Instant.now())
        );
    }

    public DataFixRunner datafixRunner(List<DataFix> actions) {
        return new DataFixRunner(actions, sqlContext(), naming(), columnValidator(), backupService(), parametersService().getHistory(datamartContext()));
    }

    public DataFixAPI dataFixApi() {
        return new DataFixAPI(sqlContext(), naming(), this);
    }

    public ColumnValidator columnValidator() {
        return new ColumnValidator(this);
    }

    public BackupService backupService() {
        return new BackupService(sqlContext(), naming(), parametersService().getToDeleteDir(), parametersService().isHistorical());
    }

    public StatisticMerger statisticMerger() {
        return new StatisticMerger();
    }

    public ReplicaNameResolver replicaNameResolver() {
        return new ReplicaNameResolver(parametersService().parentDatamartName(), parametersService().sourceSchema());
    }

    public MetastoreService metastoreService() {
        return new MetastoreService(sqlContext());
    }

    public Parametrizer parametrizer() {
        return new Parametrizer(sqlContext());
    }

    public IncrementSaveRemover incrementSaveRemover() {
        return new IncrementSaveRemoverHandler().getIncrementSaveRemover(parametersService().datamartClassName());
    }

    public DefaultDatamartFields defaultDatamartParameters() {
        return new DefaultDatamartFields(parametersService().ctlLoadingId(), parametersService().ctlStartTime().toLocalDate());
    }
}
