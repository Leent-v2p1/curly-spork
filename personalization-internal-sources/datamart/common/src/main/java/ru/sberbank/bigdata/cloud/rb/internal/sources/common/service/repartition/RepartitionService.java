package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.repartition;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.BuildRequiredChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_SCHEMA_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_TABLE_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.REPARTITIONER_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

/**
 * Класс обеспечивает репартиционирование маленьких файлов из sourceTable в файлы с размером больше fileMinSize в targetTable
 * Так же работает для непартицонированных таблиц.
 * Пример:
 * /sourceTable
 * -/month_part=2019-06
 * --/part-0000   > fileMinSize
 * --/part-0001   > fileMinSize
 * -/month_part=2019-07
 * --/part-0000   < fileMinSize
 * --/part-0001   < fileMinSize
 * будет репартицировано:
 * /targetTable
 * -/month_part=2019-06
 * --/part-0000
 * --/part-0001
 * -/month_part=2019-07
 * --/part-0000
 */
public class RepartitionService {
    public static final Logger log = LoggerFactory.getLogger(RepartitionService.class);
    private static final double FILE_MIN_SIZE = 1024 * 1024 * 1024d;
    private static final double GIGABYTE = 1024 * 1024 * 1024d;
    private static final int THREADS_NUMBER = 400;
    protected final BuildRequiredChecker buildRequiredChecker;
    private final double fileMinSize;
    private final SparkSession sqlContext;
    private final MetastoreService metastoreService;
    private final ExecutorService executorService;
    private final FullTableName sourceTable;
    private final FullTableName targetTable;
    private final TableSaveHelper tableSaveHelper;
    private final List<Throwable> exceptions;

    private RepartitionService(FullTableName sourceTable,
                               FullTableName targetTable,
                               SparkSession sqlContext,
                               BuildRequiredChecker buildRequiredChecker) {
        this(sourceTable, targetTable, sqlContext, buildRequiredChecker, FILE_MIN_SIZE);
    }

    RepartitionService(FullTableName sourceTable,
                       FullTableName targetTable,
                       SparkSession sqlContext,
                       BuildRequiredChecker buildRequiredChecker,
                       double fileMinSize) {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.sqlContext = sqlContext;
        this.fileMinSize = fileMinSize;
        this.buildRequiredChecker = buildRequiredChecker;

        this.metastoreService = new MetastoreService(sqlContext);
        this.exceptions = Collections.synchronizedList(new ArrayList<>());//делаем synchronizedList, потому что потоки могут завершаться одновременно

        this.executorService = Executors.newFixedThreadPool(THREADS_NUMBER);
        this.tableSaveHelper = new TableSaveHelper(sqlContext);
    }

    public void run() {
        if (!buildRequiredChecker.isDatamartBuildingNeeded()) {
            log.info("There is no need of RepartitionService because datamart already has been built successfully today");
            stopExecutorService();
            return;
        }
        SparkSQLUtil.dropTable(sqlContext, targetTable.fullTableName());
        sqlContext.sql("create table if not exists " + targetTable.fullTableName() + " like " + sourceTable);

        final boolean isPartitioned = SparkSQLUtil.isPartitioned(sqlContext, targetTable.fullTableName());
        if (isPartitioned) {
            List<PartitionInfo> partitions = SparkSQLUtil.getPartitions(sqlContext, sourceTable);
            log.info("Partitions in table - {} : {}", sourceTable, partitions);

            for (PartitionInfo part : partitions) {
                submitPartition(part);
            }
        } else {
            submitPartition(null);
        }

        stopExecutorService();
        if (!exceptions.isEmpty()) {
            final int numberOfExceptions = exceptions.size();
            log.error("There're {} exceptions during repartitioning: {}.\n Throwing last one of them...", numberOfExceptions, exceptions);
            throw new IllegalStateException("There's at least 1 exception.", exceptions.get(numberOfExceptions - 1));
        }
    }

    private void submitPartition(PartitionInfo partition) {
        CompletableFuture.runAsync(() -> {
            final String pathToRepartition;
            if (partition == null) {
                pathToRepartition = SparkSQLUtil.tableLocation(sqlContext, sourceTable.fullTableName())
                        .orElseThrow(() -> new IllegalStateException("Cannot find location of table " + sourceTable));
            } else {
                pathToRepartition = metastoreService.partitionLocation(sourceTable, partition)
                        .orElseThrow(() -> new IllegalStateException("Cannot find location of table " + sourceTable + " for partition " + partition));
            }

            final long partitionSizeInBytes = HDFSHelper.getDirectorySize(pathToRepartition);
            final int filesCount = HDFSHelper.filesCount(pathToRepartition);
            final String sizeInGb = String.format("%.6f", partitionSizeInBytes / GIGABYTE);
            log.info("Path {} meta stats - size in GB: {}, count: {}", pathToRepartition, sizeInGb, filesCount);

            if (partitionSizeInBytes == 0 || filesCount == 0) {
                log.info("Path {} doesnt have files - skip it", pathToRepartition);
                return;
            }

            final int coalesceByNum = (int) Math.ceil(partitionSizeInBytes / fileMinSize);
            savePartition(coalesceByNum, sourceTable, targetTable, partition, pathToRepartition);
        }, executorService).whenComplete((aVoid, throwable) -> {
            final String currentThreadName = Thread.currentThread().getName();
            final String result = partition == null ? sourceTable.toString() : partition.toString();
            log.info("complete {} in thread {}", result, currentThreadName);
            handleException(throwable);
        });
    }

    @VisibleForTesting
    void savePartition(int coalesceByNum,
                       FullTableName sourceTable,
                       FullTableName targetTable,
                       PartitionInfo partition,
                       String pathToRepartition) {
        final String condition = partition == null ? "true" : partition.whereCondition();
        log.info("Repartitioning '{}' with coalesce = {}", pathToRepartition, coalesceByNum);
        final Dataset<Row> sourceDf = sqlContext
                .table(sourceTable.fullTableName())
                .where(condition)
                .coalesce(coalesceByNum);//if coalesceByNum > filesCount then spark wouldn't do anything, so it's okay to use it

        tableSaveHelper.insertOverwriteTable(targetTable.fullTableName(), sourceDf, partition);
    }

    private void handleException(Throwable throwable) {
        if (throwable != null) {
            //добавление в лист нужно затем, что бы бросить исключение из main thread, а не current thread
            //whenComplete() выполняется в том же потоке что и runAsync()
            final String exception = throwable.toString();
            log.warn("add {} to exception list", exception);
            exceptions.add(throwable.getCause());
        }
    }

    private void stopExecutorService() {
        log.info("shutdown executorService");
        executorService.shutdown();
        while (true) {
            try {
                if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException ex) {
                log.warn("Thread {} was interrupted during shutdown of executorService!", Thread.currentThread());
                Thread.currentThread().interrupt();
            }
        }
        log.info("shutdown of executorService ended");
    }

    public static void main(String[] args) {
        final String resultSchema = safeSystemProperty(RESULT_SCHEMA_PROPERTY);
        final String resultTable = safeSystemProperty(RESULT_TABLE_PROPERTY);

        final FullTableName datamartId = FullTableName.of(resultSchema, resultTable);
        LoggerTypeId.set(datamartId.fullTableName() + REPARTITIONER_POSTFIX);
        final String jobName = "repartitioning-service-" + datamartId;

        final Map<String, String> sparkProperties = new HashMap<>();
        sparkProperties.put("spark.scheduler.mode", "FAIR");
        sparkProperties.put("spark.scheduler.allocation.file", "scheduler-pool.xml");
        final Map<String, String> sparkLocalProperties = Collections.singletonMap("spark.scheduler.pool", "fair_pool");

        final DatamartServiceFactory serviceFactory = new DatamartServiceFactory(datamartId, jobName, sparkProperties, sparkLocalProperties);

        final DatamartNaming naming = serviceFactory.naming();
        final RepartitionService repartitionService = new RepartitionService(
                FullTableName.of(naming.reserveFullTableName()),
                FullTableName.of(naming.repartitionedFullTableName()),
                serviceFactory.sqlContext(),
                serviceFactory.buildRequiredChecker()
        );

        repartitionService.run();
    }
}
