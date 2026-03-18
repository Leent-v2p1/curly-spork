package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.data_quality_check;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.EmergencyStopRequiredChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;

import java.util.*;
import java.util.stream.Collectors;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_SCHEMA_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_TABLE_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.STG_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.getSystemProperty;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

/**
 * ПРИНЦИП РАБОТЫ ПРИЛОЖЕНИЯ<br>
 * 1. Прочитать файл с hql (два файла - один с мониторингами, второй со стоп-кранами) //мониторинги пока не работают, нужно отдельно добавить <br>
 *  1.1 если ни одного файла не найдено, считать этап успешно завершенным<br>
 *  1.2 если есть файл с стоп-краном, обрывать поток при наличии проверок с false и выдавать их как лог (только при наличии опции spark.emergencyStop: true<br>
 *  1.3 если есть файл с мониторингами, то выдавать их результат в виде файла hdfs или как таблицу. //пока нет такого функционала <br>
 * 2. Разбить на части по шаблону <br>
 *  2.1 в качестве шаблон  --//-- <br>
 *  2.2 поддерживаются комментарии и многострочные запросы, главное чтобы возвращали Boolean<br>
 * 3. Последовательно исполняется каждая команда<br>
 *  3.1 в случае ошибки отправляет текст проверки и результат в отдельный список и отбрасывает его в лог с завершением работы потока<br>
 */

public class DataQualityCheckService {
    public static final Logger log = LoggerFactory.getLogger(DataQualityCheckService.class);
    protected final EmergencyStopRequiredChecker emergencyStopRequiredChecker;
    private final SparkSession sqlContext;
    private final MetastoreService metastoreService;
    private final FullTableName stgTable;
    private final FullTableName tableName;
    private final boolean kafkaSaverRequired;

    DataQualityCheckService(FullTableName stgTable,
                            FullTableName tableName,
                            SparkSession sqlContext,
                            EmergencyStopRequiredChecker emergencyStopRequiredChecker,
                            boolean kafkaSaverRequired) {
        this.stgTable = stgTable;
        this.tableName = tableName;
        this.sqlContext = sqlContext;
        this.emergencyStopRequiredChecker = emergencyStopRequiredChecker;
        this.kafkaSaverRequired = kafkaSaverRequired;
        this.metastoreService = new MetastoreService(sqlContext);
    }

    public void run() {
        if (!emergencyStopRequiredChecker.isEmergencyStopRequired()) {
            log.info("There is no need for Emergency stop");
            return;
        }

        String dqPath = new PathBuilder(Environment.PRODUCTION).resolveEmergencyStopFile(tableName);
        final Optional<String> dqRequestFromFile = readRequestsFromHql(dqPath);
        //в случае если файла проверки не существует, этап завершается успехом с соответствующим логированием
        String dqFile;
        if (dqRequestFromFile.isPresent()) {
            dqFile = dqRequestFromFile.get();
        } else {
            return;
        }

        if (!metastoreService.tableExists(stgTable)) {
            log.error("There is no table to check, skip DQCheck step");
            throw new DataQualityCheckException("Table " + stgTable + " does not exist");
        }

        if (sqlContext.sql("select * from " + stgTable.fullTableName()).isEmpty()) {
            log.error("The table {} is empty, skip DQCheck step", stgTable);
            if (!kafkaSaverRequired) {
                throw new DataQualityCheckException("Table " + stgTable + " is empty");
            } else {
                log.info("Increment is empty. DQCheck step is skipped.");
                return;
            }
        }

        final List<String> checkRequests = getRequestsFromString(dqFile);
        checkRequests.forEach(query -> log.info("Request: \"" + query + "\""));

        final List<RequestMessage> errors = checkRequests
                .stream().map(query -> {
                    try {
                        Dataset<Row> resultFlagDataset = sqlContext.sql(query);
                        Boolean resultFlag = resultFlagDataset.first().getAs(0);
                        return new RequestMessage(query, resultFlag);
                    } catch (Exception e) {
                        log.warn("Exception occurred during DQCheck step, request is ignored: {}", new RequestMessage(query, false));
                        log.warn(e.getMessage());
                        return new RequestMessage(query, true);
                    }
                })
                .filter(message -> !message.result)
                .collect(Collectors.toList());
        errors.forEach(e -> log.warn(e.toString()));

       if (!errors.isEmpty()) {
            throw new DataQualityCheckException("Number of not passed checks: " + errors.size());
        } else {
            log.info("Checks passed, no emergency stop required");
        }
    }

    public static void main(String[] args) {
        final String resultSchema = safeSystemProperty(RESULT_SCHEMA_PROPERTY) + STG_POSTFIX;
        final String resultTable = safeSystemProperty(RESULT_TABLE_PROPERTY);

        final FullTableName datamartId = FullTableName.of(resultSchema, resultTable);
        LoggerTypeId.set(datamartId.fullTableName());
        final String jobName = "dqcheck-service-" + datamartId;

        final Map<String, String> sparkProperties = new HashMap<>();
        sparkProperties.put("spark.scheduler.mode", "FAIR");
        sparkProperties.put("spark.scheduler.allocation.file", "scheduler-pool.xml");
        final Map<String, String> sparkLocalProperties = Collections.singletonMap("spark.scheduler.pool", "fair_pool");

        final DatamartServiceFactory serviceFactory = new DatamartServiceFactory(datamartId, jobName, sparkProperties, sparkLocalProperties);

        DatamartNaming naming;
        try {
            naming = serviceFactory.naming();
        } catch(Exception e) {
            log.warn("Exception occurred during DQCheck naming step, step is ignored: ");
            log.warn(e.getMessage());
            return;
        }

        boolean kafkaSaverRequired = serviceFactory.kafkaSaverRequiredChecker().isKafkaSaverRequired();

        final DataQualityCheckService dqcheckService = new DataQualityCheckService(
                FullTableName.of(naming.reserveFullTableName()),
                FullTableName.of(naming.fullTableName()),
                serviceFactory.sqlContext(),
                serviceFactory.emergencyStopRequiredChecker(),
                kafkaSaverRequired);

        dqcheckService.run();
    }

    private Optional<String> readRequestsFromHql(String dqPath) {
        try {
            final String dqFile = HDFSHelper.readFileAsString(dqPath);
            log.info("Read dqfile: " + dqPath);
            log.info(dqFile);
            return Optional.of(dqFile);
        } catch (Exception e) {
            log.warn("Exception occured while read dqcheck hql file: ", e);
            log.warn("DQCheck step is skipped");
            return Optional.empty();
        }
    }

    @VisibleForTesting
    protected static List<String> getRequestsFromString(String dqFile) {
        final List<String> checkRequests = Arrays.stream(dqFile.concat("\n").split("--//--")).filter(e -> !e.isEmpty())
                .map(line -> line.replaceAll("--\\S+\\n", "")) //удаляем комментарии из строк hql
                .map(String::trim)
                .filter(line -> !line.startsWith("--")) //удаляем строки с оставшимися комментариями
                .map(query -> query.replace("\n", " "))
                .collect(Collectors.toList());
        return checkRequests;
    }

    private static class RequestMessage {
        final String request;
        final Boolean result;

        public RequestMessage(String request, Boolean result) {
            this.request = request;
            this.result = result;
        }

        @Override
        public String toString() {
            return "Data Quality Check result is: " + (result ? "PASSED" : "FAILED")
                    + " for request: \n" + request;
        }
    }
}


