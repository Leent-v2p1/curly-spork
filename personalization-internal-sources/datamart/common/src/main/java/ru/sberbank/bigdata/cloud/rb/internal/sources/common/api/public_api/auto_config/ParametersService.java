package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config;

import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.ToDeleteResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.EpkSystemCode;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.MdmSystemCode;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.HistoricalDatamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.ReplicaBasedHistoricalDatamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.history.History;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.ToDeleteDir;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DateConstants;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest.CtlLoadingStatus;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.LoadingStatus;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Profile;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Statistic;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.CodTerbankCode;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.TerbankCodeEpkCodeMapper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.TerbankCodeMdmCodeMapper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes.CodDailyByInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes.EribDailyByInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.Checker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.StringUtils.toInt;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.CollectionUtils.extractExactlyOneOptional;

/**
 * Class contains parameters that calculated in runtime from different sources: Ctl, system-properties and so on.
 */
public class ParametersService {

    private static final Logger log = LoggerFactory.getLogger(DatamartServiceFactory.class);

    private final FullTableName datamartId;
    private final Environment env;
    private final ToDeleteResolver toDeleteResolver;
    private final CtlApiCalls ctlRestApi;

    private final SparkSession sqlContext;

    public ParametersService(FullTableName datamartId,
                             Environment env,
                             ToDeleteResolver toDeleteResolver,
                             CtlApiCalls ctlRestApi,
                             SparkSession sqlContext) {
        this.datamartId = datamartId;
        this.env = env;
        this.toDeleteResolver = toDeleteResolver;
        this.ctlRestApi = ctlRestApi;
        this.sqlContext = sqlContext;
    }

    public MdmSystemCode codMdmSystemCode() {
        final String postfix = codSourcePostfix().getPostfix();
        final int terbankCodePostfix = Integer.parseInt(postfix);
        final CodTerbankCode terbankCode = CodTerbankCode.getByCode(terbankCodePostfix);
        final MdmSystemCode mdmSystemCode = new TerbankCodeMdmCodeMapper().map(terbankCode);
        log.info("got {} mdmSystemCode from cod postfix {}", mdmSystemCode, postfix);
        return mdmSystemCode;
    }

    public List<EpkSystemCode> codEpkSystemCode() {
        final String postfix = codSourcePostfix().getPostfix();
        final int terbankCodePostfix = Integer.parseInt(postfix);
        final CodTerbankCode terbankCode = CodTerbankCode.getByCode(terbankCodePostfix);
        final List<EpkSystemCode> epkSystemCode = new TerbankCodeEpkCodeMapper().map(terbankCode);
        log.info("got {} epkSystemCode from cod postfix {}", epkSystemCode, postfix);
        return epkSystemCode;
    }

    @SneakyThrows
    public LocalDateTime ctlStartTime() {
        final List<LoadingStatus> loadingStatuses = ctlRestApi.getLoading(ctlLoadingId()).loading_status;
        return loadingStatuses
                .stream()
                .filter(ls -> CtlLoadingStatus.RUNNING.getCtlRestApiValue().equals(ls.status))
                .map(ls -> LocalDateTime.parse(ls.effective_from.split("\\.")[0], DateConstants.EFFECTIVE_TIME_FORMAT))
                .min(LocalDateTime::compareTo)
                .orElseThrow(() -> new IllegalStateException("Cannot find 'RUNNING' status in " + loadingStatuses));
    }

    public LocalDate ctlBuildDate() {
        String customBuildDate = getSystemProperty("spark.custom.build.date");
        if (customBuildDate != null && !customBuildDate.isEmpty()) {
            return LocalDate.parse(customBuildDate);
        }
        return ctlStartTime().toLocalDate();
    }

    public Environment getEnv() {
        return env;
    }

    public Optional<LocalDate> recoveryDate() {
        return Optional.ofNullable(SysPropertyTool.getSystemProperty("spark.recovery.date"))
                .filter(s -> !s.isEmpty())
                .map(LocalDate::parse);
    }

    public LocalDate replicaActualityDate() {
        if (recoveryMode()) {
            return recoveryDate().orElseThrow(IllegalStateException::new);
        } else {
            return ctlBuildDate().minusDays(1);
        }
    }

    public boolean isFirstLoading() {
        Optional<LocalDate> ctlBuildDateValue = getBusinessDateValue();
        log.info("ctlBuildDateValue = {}", ctlBuildDateValue);
        final boolean resultTableExists = SparkSQLUtil.isTableExists(sqlContext, datamartId);
        log.info("resultTableExists ({}) = {}", datamartId, resultTableExists);
        final boolean forceInitLoad = forceInitLoad();
        log.info("forceInitLoad ({}) = {}", datamartId, forceInitLoad);
        final boolean isFirstLoading = !ctlBuildDateValue.isPresent() || !resultTableExists || forceInitLoad;
        log.info("isFirstLoading = {}", isFirstLoading);
        return isFirstLoading;
    }

    public boolean forceInitLoad() {
        return dateParameterEqualsNow("init_load");
    }

    public boolean skipIfBuiltToday() {
        boolean skipIfBuiltToday = Boolean.parseBoolean(safeSystemProperty("spark.skipIfBuiltToday"));
        log.info("skipIfBuiltToday from property: {}", skipIfBuiltToday);
        boolean result = !isFirstLoading() && skipIfBuiltToday;
        log.info("skipIfBuiltToday considering firstLoading: {}", result);
        return result;
    }

    public Optional<LocalDate> startCtlParameter() {
        return isDatesToFilterNeeded()
                ? getCtlDateParameter("start_dt")
                : Optional.empty();
    }

    public Optional<LocalDate> endCtlParameter() {
        return isDatesToFilterNeeded()
                ? getCtlDateParameter("end_dt")
                : Optional.empty();
    }

    public boolean forceReload() {
        return dateParameterEqualsNow("reload");
    }

    protected boolean dateParameterEqualsNow(String parameter) {
        final String fullParamName = "spark." + parameter;
        final String param = getSystemProperty(fullParamName);
        log.info("{} from property: {}", parameter, param);
        if (param != null) {
            final LocalDate paramValue = LocalDate.parse(param);
            final LocalDate currentDate = ctlStartTime().toLocalDate();
            log.info("currentDate: {}, {} date: {}", currentDate, parameter, paramValue);
            return currentDate.equals(paramValue);
        } else {
            log.info("{} is empty", fullParamName);
            return false;
        }
    }

    public Optional<LocalDate> getCtlDateParameter(String dateParameterName) {
        String date = "";
        try {
            date = getSystemProperty("spark." + dateParameterName);
            final LocalDate result = LocalDate.parse(date);
            log.info("{} from property: {}", dateParameterName, date);
            return Optional.of(result);
        } catch (NullPointerException e) {
            log.info("{} not found", dateParameterName);
            return Optional.empty();
        } catch (DateTimeParseException e){
            log.info("{} could not be parsed: '{}'", dateParameterName, date);
            return Optional.empty();
        }
    }

    public boolean isDatesToFilterNeeded() {
        final boolean result = forceReload() || forceInitLoad();
        if (result) {
            log.info("Parameters 'start_dt' and 'end_dt' are needed");
        } else {
            log.info("Parameters 'start_dt' and 'end_dt' are not needed");
        }
        return result;
    }

    @SneakyThrows
    public String ctlProfile() {
        String profile = ctlRestApi.getLoading(ctlLoadingId()).profile;
        log.info("ctl profile: {}", profile);
        return profile;
    }

    @SneakyThrows
    public Optional<LocalDate> getBusinessDateValue() {
        final Optional<Integer> entityId = ctlEntityIdOptional();
        if (!entityId.isPresent()) {
            return Optional.empty();
        }
        return ctlRestApi.getStatisticAsLocalDate(entityId.get(), ctlProfile(), BUSINESS_DATE_STAT_ID.getCode());
    }
@SneakyThrows
    public List<Statistic> getAllStats(int entityId, StatisticId statId){
        return ctlRestApi.getAllStats(entityId, statId.getCode());
    }

    public Long getLastProcessedLoadingId() {
        return getLastStatistic(PROCESSED_LOADING_ID, statistic -> Long.valueOf(statistic.value));
    }

    @SneakyThrows
    public Optional<Statistic> getFirstRollback(int entityId, long lastLoading){
        return getAllStats(entityId, ROLLBACK)
                .stream()
                .filter(stat -> stat.loading_id > lastLoading)
                .findFirst();
    }

    /**
     * Использовать если значение статистикик сразу представлено в формате Timestamp
     */
    public Timestamp getLastTimestampStatValue(StatisticId statId) {
        return getLastStatistic(statId, statistic -> Timestamp.valueOf(statistic.value));
    }

    /**
     * Использовать если значение статистики надо модифицировать перед получением Timestamp из этого значения
     */
    public Timestamp getLastTimestampStatValue(StatisticId statId, Function<Statistic, LocalDateTime> mapFunction) {
        return getLastStatistic(statId, statistic -> Timestamp.valueOf(mapFunction.apply(statistic)));
    }

    public <T> T getLastStatistic(StatisticId statId, Function<Statistic, T> mapFunction) {
        final Optional<Statistic> lastStatistic = getLastStatistic(statId);
       log.info("Last ctl statistic {} for entity with id {}, is - {}", statId, ctlEntityId(), lastStatistic);
        final T lastStatisticValue = getLastStatistic(statId)
                .map(mapFunction)
                .orElseThrow(() -> new IllegalStateException("Last statistic №" + statId + " for entity " + ctlEntityId() + " not found"));
        log.info("Last ctl statistic {} for entity with id {}, is - {}", statId, ctlEntityId(), lastStatisticValue);
        return lastStatisticValue;
    }
@SneakyThrows
    public <T> T getLastStatistic(StatisticId statId, int entityId, Function<Statistic, T> mapFunction){
        final T lastStatisticValue = getLastStatistic(statId, entityId)
                .map(mapFunction)
                .orElseThrow(() -> new IllegalStateException("Last statistic №" + statId + " for entity " + entityId + " not found"));
        log.info("Last ctl statistic {} for entity with id {}, is - {}", statId, ctlEntityId(), lastStatisticValue);
        return lastStatisticValue;
    }

    @SneakyThrows
    public Optional<Statistic> getLastStatistic(StatisticId statId) {
        final Optional<Integer> entityId = ctlEntityIdOptional();
        if (!entityId.isPresent()) {
            return Optional.empty();
        }
        return ctlRestApi.getLastStatisticWithProfile(entityId.get(), statId.getCode(), ctlProfile());
    }

    @SneakyThrows
    public String getHueLink() {
        Profile profileString = ctlRestApi.getProfile(ctlProfile());
        return profileString.hueUri;
    }

    @SneakyThrows
    public Optional<Statistic> getLastStatistic(StatisticId statId, int entityId) throws JSONException, IOException {
        return ctlRestApi.getLastStatistic(entityId, statId.getCode());
    }

    public Integer ctlEntityId() {
        return ctlEntityIdOptional()
                .orElseThrow(() -> new IllegalStateException("ctlEntityId is incorrect for " + datamartId));
    }

    public Optional<Integer> ctlEntityIdOptional() {
        return getIntSystemProperty(SPARK_OPTION_PREFIX + CTL_ENTITY_ID_PROPERTY);
    }

    public boolean repartition() {
        return ENABLED_FLAG.equals(getSystemProperty(SPARK_OPTION_PREFIX + REPARTITION_PROPERTY));
    }

    public boolean recoveryMode() {
        final Optional<LocalDate> recoveryDateProperty = recoveryDate();
        if (!recoveryDateProperty.isPresent()) {
            log.info("Property recovery.date is empty, so it is not a recovery mode");
            return false;
        }
        if (getEnv().isTestEnvironment()) {
            log.info("For test environment recovery mode is true");
            return true;
        }

        final LocalDate recoveryDate = recoveryDateProperty.get();
        Checker.checkCondition(!recoveryDate.equals(ctlBuildDate()), "recovery date and ctl build date should be equal");

        final boolean recoveryMode = ctlBuildDate().isBefore(LocalDate.now());
        Checker.checkCondition(!recoveryMode, "recoveryDate should be empty or should be before LocalDate.now()");

        log.info("Building datamart {} in recoveryMode: {}", datamartId, recoveryMode);
        return true;//TODO APID1-179 method is always returns true, which is probably wrong
    }

    public String userHomePath() {
        return SysPropertyTool.getUserHomePathChecked();
    }

    public List<CodTerbankCode> codTerbankCodes() {
        return asList(CodTerbankCode.values());
    }

    public EribDailyByInstance eribSourcePostfix() {
        return (EribDailyByInstance) sourcePostfix()
                .orElseThrow(() -> new IllegalArgumentException("Can't find erib sourcePostfix " + datamartId));
    }

    public CodDailyByInstance codSourcePostfix() {
        return (CodDailyByInstance) sourcePostfix()
                .orElseThrow(() -> new IllegalArgumentException("Can't find cod sourcePostfix " + datamartId));
    }

    public Optional<SourcePostfix> sourcePostfix() {
        final SchemaAlias schemaAlias = SchemaAlias.of(targetSchema());
        final List<SourcePostfix> sourcePostfixesForSchema = SOURCE_POSTFIXES_FOR_SOURCE.getOrDefault(schemaAlias, emptyList());
        return extractExactlyOneOptional(sourcePostfixesForSchema,
                postfix -> targetTable().endsWith(postfix.getPostfix())
        );
    }

    public Integer ctlLoadingId() {
        return toInt(getSystemProperty("spark.ctl.loading.id"));
    }

    public String datamartClassName() {
        return safeSystemProperty("spark.datamart.class.name");
    }

    public String sourceSchema() {
        String sourceSchema = getSystemProperty("spark.source.schema");
        if (sourceSchema == null) {
            WorkflowType workflowType = workflowType();
            if (workflowType.equals(WorkflowType.DATAMART) || workflowType.equals(WorkflowType.STAGE)) {
                throw new IllegalArgumentException("spark.source.schema must be non empty for datamart");
            }
        }
        return sourceSchema;
    }

    public WorkflowType workflowType() {
        return WorkflowType.valueOfByKey(getSystemProperty("spark.workflow.type"));
    }

    public Optional<Integer> monthlyDependencyDay() {
        return getIntSystemProperty("spark.monthly.dependency.day");
    }

    public FullTableName parentDatamartName() {
        return FullTableName.of(safeSystemProperty("spark.datamart.parent.table.name"));
    }

    public boolean emergencyStopRequired() {
        return Boolean.parseBoolean(getSystemProperty("spark.emergencyStop", "false"));
    }

    public boolean kafkaSaverRequired() {
        return Boolean.parseBoolean(getSystemProperty("spark.kafkaSaver", "false"));
    }

    public ToDeleteDir getToDeleteDir() {
        return toDeleteResolver.resolve(datamartClassName());
    }

    public String targetSchema() {
        return datamartId.dbName();
    }

    public String targetTable() {
        return datamartId.tableName();
    }

    public String loadingTypeStat() {
        List<String> params = new ArrayList<>();

        params.add(isFirstLoading() ? "init" : "inc");
        startCtlParameter().map(date -> "start_dt=" + date).ifPresent(params::add);
        endCtlParameter().map(date -> "end_dt=" + date).ifPresent(params::add);
        log.info("Result params for loadingType statistic is: {}", params);

        return String.join(";", params);
    }

    public boolean shouldUpdateMonthStat() {
        Optional<Integer> dayToRunMonthWf = monthlyDependencyDay();
        log.info("Checking if there is need for {} monthly statistic update.", datamartId);
        if (dayToRunMonthWf.isPresent()) {
            final int dayToRun = dayToRunMonthWf.get();
            final int currentDayOfMonth = ctlStartTime().getDayOfMonth();
            boolean shouldUpdateMonthStat = dayToRun == currentDayOfMonth;
            log.info("Required day is {}. Current day is {}, from ctl.start.time ({}). So result is {}", dayToRun, currentDayOfMonth, ctlStartTime(), shouldUpdateMonthStat);
            return shouldUpdateMonthStat;
        } else {
            log.info("{} doesn't have monthly datamart dependencies", datamartId);
            return false;
        }
    }

    public boolean isHistorical() {
        try {
            final Class<?> parentClass = this.getClass().getClassLoader().loadClass(datamartClassName()).getSuperclass();
            return parentClass.equals(HistoricalDatamart.class) || parentClass.equals(ReplicaBasedHistoricalDatamart.class);
        } catch (ClassNotFoundException e) {
            final String message = String.format("Some problem with class %s", datamartClassName());
            throw new IllegalArgumentException(message, e);
        }
    }

    public Optional<History> getHistory(DatamartContext datamartContext) {
        if (isHistorical()) {
            final Class<?> datamartClass;
            try {
                datamartClass = this.getClass().getClassLoader().loadClass(datamartClassName());
                final HistoricalDatamart historicalDatamart = (HistoricalDatamart) datamartClass.newInstance();
                historicalDatamart.setReplicaActualityDate(LocalDate.now());
                historicalDatamart.setDc(datamartContext);
                return Optional.of(historicalDatamart.buildHistory());
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                final String message = String.format("Some problem with class %s", datamartClassName());
                throw new IllegalArgumentException(message, e);
            }
        }
        return Optional.empty();
    }
}
