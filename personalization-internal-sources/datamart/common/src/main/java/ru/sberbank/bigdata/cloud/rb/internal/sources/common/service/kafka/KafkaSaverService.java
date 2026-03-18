package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.KafkaSaverRequiredChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.*;

import static org.apache.spark.sql.functions.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_SCHEMA_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_TABLE_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.*;

public class KafkaSaverService {
    public static final Logger log = LoggerFactory.getLogger(KafkaSaverService.class);
    protected final KafkaSaverRequiredChecker kafkaSaverRequiredChecker;
    private final SparkSession sqlContext;
    private final MetastoreService metastoreService;
    private final FullTableName sourceTable;
    private final String kafkaTopic;
    private final String kafkaServer;
    private final String kafkaCertName;
    private final boolean testStandKafkaFlag;
    private final KafkaBigDataParams KafkaBigDataParams;

    private KafkaSaverService(FullTableName sourceTable,
                               SparkSession sqlContext,
                               KafkaSaverRequiredChecker kafkaSaverRequiredChecker,
                               String kafkaTopic,
                               String kafkaServer,
                               String kafkaCertName,
                              boolean testStandKafkaFlag,
                              KafkaBigDataParams KafkaBigDataParams) {
        this.sourceTable = sourceTable;
        this.sqlContext = sqlContext;
        this.metastoreService = new MetastoreService(sqlContext);
        this.kafkaSaverRequiredChecker = kafkaSaverRequiredChecker;
        this.kafkaTopic = kafkaTopic;
        this.kafkaServer = kafkaServer;
        this.kafkaCertName = kafkaCertName;
        this.testStandKafkaFlag = testStandKafkaFlag;
        this.KafkaBigDataParams=KafkaBigDataParams;
    }

    public void run() {
        if (!kafkaSaverRequiredChecker.isKafkaSaverRequired() || !testStandKafkaFlag) {
            log.info("There is no need for KafkaSaver");
            return;
        }

        if (!metastoreService.tableExists(sourceTable)) {
            log.error("There is no table to check, skip KafkaSaver step");
            throw new KafkaSaverException("Table " + sourceTable + " does not exist");
        }

        Dataset<Row> sourceTableDf = sqlContext.table(sourceTable.fullTableName());
        Dataset<Row> kafkaTable = sourceTableDf
                .select(to_json(struct(col("*"))).as("value"));

        if (!(KafkaBigDataParams.getKafkaBigDataFlag())) {
            try {
                kafkaTable
                        .write()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", kafkaServer)
                        .option("includeHeaders", "true")
                        .option("kafka.enable.idempotence", "false")
                        .option("topic", kafkaTopic)
                        .option("kafka.security.protocol", "SSL")
                        .option("kafka.ssl.endpoint.identification.algorithm", "")
                        .option("kafka.ssl.truststore.location", kafkaCertName)
                        .option("kafka.ssl.truststore.password", "")
                        .option("kafka.ssl.keystore.location", kafkaCertName)
                        .option("kafka.ssl.keystore.password", "")
                        .option("kafka.ssl.key.password", "")
                        .save();
                log.info("Messages from table {} has been successfully sent using KafkaSaver.", sourceTable.fullTableName());

            } catch (Exception e) {
                log.warn("Exception occurred during KafkaSaver step.");
                throw new KafkaSaverException(e.getMessage());
            }
        }
        else
        {
            if (Arrays.stream(sourceTableDf.schema().fieldNames()).anyMatch(t -> t.equals("part_id")))

            {
                long CountParts = (sourceTableDf.select(col("part_id"))
                    .distinct()).count();

                for (Integer num = KafkaBigDataParams.getStartIncPartid(); num <= KafkaBigDataParams.getEndIncPartid() && num < CountParts; num++) {
                    try {
                        Dataset<Row> sourceTableDfParts = sourceTableDf
                                .where(col("part_id").equalTo(num))
                                .drop(col("part_id"));
                        sourceTableDfParts
                                .select(to_json(struct(col("*"))).as("value"))
                                .write()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", kafkaServer)
                                .option("includeHeaders", "true")
                                .option("kafka.enable.idempotence", "false")
                                .option("topic", kafkaTopic)
                                .option("kafka.security.protocol", "SSL")
                                .option("kafka.ssl.endpoint.identification.algorithm", "")
                                .option("kafka.ssl.truststore.location", kafkaCertName)
                                .option("kafka.ssl.truststore.password", "")
                                .option("kafka.ssl.keystore.location", kafkaCertName)
                                .option("kafka.ssl.keystore.password", "")
                                .option("kafka.ssl.key.password", "")
                                .save();
                        log.info("Messages from table {} has been successfully sent using KafkaSaver.", sourceTable.fullTableName());
                        if (num!=KafkaBigDataParams.getEndIncPartid() && num!=(CountParts-1)) {
                            Thread.sleep(KafkaBigDataParams.getIntervalInc());
                            }
                    } catch (Exception e) {
                        log.warn("Exception occurred during KafkaSaver step.");
                        throw new KafkaSaverException(e.getMessage());
                    }
                }
            }
            else
            {
                log.warn("Attention!!! KafkaSaverService didn't find column PART_ID, please, set parameter kafkaBigDataFlag false");
            }

        }
    }

    public static void main(String[] args) {
        final String sourceSchema = safeSystemProperty(RESULT_SCHEMA_PROPERTY);
        final String sourceTable = safeSystemProperty(RESULT_TABLE_PROPERTY);

        final String kafkaTopic = safeSystemProperty("spark.kafka.topic");
        final String kafkaServer = safeSystemProperty("spark.kafka.server");
        final String kafkaCertName = safeSystemProperty("spark.kafka.certName");
        final Boolean kafkaBigDataFlag = Boolean.parseBoolean(getSystemProperty("spark.kafkaBigDataFlag", "false"));
        final Integer maxInc = Integer.parseInt(getSystemProperty("spark.maxIncCount", "15000000")); //по умолчанию 15 млн записей
        final Integer intervalInc = Integer.parseInt(getSystemProperty("spark.intervalInc", "14400000")); // по умолчанию 4 часа в милисекундах
        final Integer endIncPartid = Integer.parseInt(getSystemProperty("spark.endIncPartid", "6"));
        final Integer startIncPartid = Integer.parseInt(getSystemProperty("spark.startIncPartid", "0"));

        // для тестовых стендов должен быть тестовый топик
        final String userName = safeSystemProperty("spark.user.name");
        final boolean testStandFlag = (userName.startsWith("u_ut"));
        final boolean testTopicFlag = Arrays.stream(TestTopicName.values())
                .anyMatch((t) -> t.getTopicName().equals(kafkaTopic));
        final boolean testStandKafkaFlag = !testStandFlag || testTopicFlag;

        final FullTableName datamartId = FullTableName.of(sourceSchema, sourceTable);
        LoggerTypeId.set(datamartId.fullTableName() + KAFKA_SAVER_POSTFIX);
        final String jobName = "kafka-service-" + datamartId;

        final Map<String, String> sparkProperties = new HashMap<>();
        sparkProperties.put("spark.scheduler.mode", "FAIR");
        sparkProperties.put("spark.scheduler.allocation.file", "scheduler-pool.xml");
        final Map<String, String> sparkLocalProperties = Collections.singletonMap("spark.scheduler.pool", "fair_pool");

        final DatamartServiceFactory serviceFactory = new DatamartServiceFactory(datamartId, jobName, sparkProperties, sparkLocalProperties);
        final KafkaBigDataParams KafkaBigDataParams = new KafkaBigDataParams(kafkaBigDataFlag, maxInc, intervalInc, endIncPartid, startIncPartid);
        final DatamartNaming naming = serviceFactory.naming();

        final KafkaSaverService kafkaSaverService = new KafkaSaverService(
                FullTableName.of(naming.kafkaFullTableName()),
                serviceFactory.sqlContext(),
                serviceFactory.kafkaSaverRequiredChecker(),
                kafkaTopic,
                kafkaServer,
                kafkaCertName,
                testStandKafkaFlag,
                KafkaBigDataParams
        );

        kafkaSaverService.run();
    }
}