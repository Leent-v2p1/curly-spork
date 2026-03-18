package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save;

import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.SeparateHistoryTableSavingStrategy.HISTORY_PARTITIONING;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.SeparateHistoryTableSavingStrategy.SNAPSHOT_PARTITIONING;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.STG_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.ENABLED_FLAG;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.SAVE_SALTING_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.SPARK_OPTION_PREFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper.getTemplate;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.formatPartitions;

/**
 * утилитные методы для сохранения и обновления партиционированных витрин
 */
public class TableSaveHelper {

    private static final Logger log = LoggerFactory.getLogger(TableSaveHelper.class);
    public static final String SALT_COLUMN = "salt_index";
    private static final int SALT_ROWS = 500_000;
    private static final String SALT_TABLE_POSTFIX = "_self_salt";

    private final SparkSession context;
    private final DataFrameSaver dataFrameSaver;

    public TableSaveHelper(SparkSession context) {
        this.context = context;
        this.dataFrameSaver = new SparkDataFrameSaver(context);
    }

    /**
     * Creates table with partitions of string type
     */
    public void createPartitionedTable(StructType schema, String fullTableName, PartitionInfo partitionInfo) {
        createPartitionedTable(
                getStructTypeWithoutPartition(partitionInfo.colNames(), schema),
                fullTableName,
                formatPartitions(partitionInfo.partitions())
        );
    }

    public void createTable(StructType schema, String fullTableName) {
        createPartitionedTable(schema, fullTableName, (String) null);
    }

    /**
     * Method creates targetTable with same schema and partitions from sourceTable
     */
    public void createCopyTable(String sourceTable, String targetTable) {
        final Optional<PartitionInfo> partitionInfo = SparkSQLUtil.getPartitionInfo(context, sourceTable);
        final StructType sourceSchema = context.table(sourceTable).schema();
        if (partitionInfo.isPresent()) {
            createPartitionedTable(sourceSchema, targetTable, partitionInfo.get());
        } else {
            createTable(sourceSchema, targetTable);
        }
    }

    public void createPartitionedReserveTable(StructType reserveSchema,
                                              String reserveFullTableName,
                                              String paTableName,
                                              PartitionInfo partitionInfo) {
        Optional<StructType> paSchema = getMetastorePaSchema(paTableName);
        StructType resultSchema;
        log.info("spark.init_load: {}", System.getProperty("spark.init_load"));
        if (paSchema.isPresent() && System.getProperty("spark.init_load") == null) {
            StructType reserveStructType = addPartitionColumnsToSchema(partitionInfo.colNames(), reserveSchema);
            StructType paStructType = paSchema.get();
            checkReserveAndPaSchemasCompatibility(paStructType, reserveStructType);
            resultSchema = paStructType;
        } else {
            resultSchema = reserveSchema;
        }
        createPartitionedTable(getStructTypeWithoutPartition(partitionInfo.colNames(), resultSchema),
                reserveFullTableName,
                formatPartitions(partitionInfo.partitions()));
    }

    public void createReserveTable(StructType reserveSchema, String fullTableName, String paTableName) {
        Optional<StructType> paSchema = getMetastorePaSchema(paTableName);
        StructType resultSchema;
        log.info("spark.init_load: {}", System.getProperty("spark.init_load"));
        if (paSchema.isPresent()  && System.getProperty("spark.init_load") == null) {
            StructType paStructType = paSchema.get();
            checkReserveAndPaSchemasCompatibility(paStructType, reserveSchema);
            resultSchema = paStructType;
        } else {
            resultSchema = reserveSchema;
        }
        createPartitionedTable(resultSchema, fullTableName, (String) null);
    }

    private Optional<StructType> getMetastorePaSchema(String paTableName) {
        log.info("paTableName = {}", paTableName);
        if (SparkSQLUtil.isTableExists(context, FullTableName.of(paTableName))) {
            log.info("{} is exists", paTableName);
            return Optional.of(context.table(paTableName).schema());
        }
        return Optional.empty();
    }

    private void checkReserveAndPaSchemasCompatibility(StructType paStructType, StructType reserveStructType) {
        List<String> reserveColumnNames = new ArrayList<>(asList(reserveStructType.fieldNames()));
        final String reserveColNamesString = String.join(",", reserveColumnNames);
        log.info("reserveColumnNames: {}", reserveColNamesString);
        List<String> paMetastoreColumnNames = new ArrayList<>(asList(paStructType.fieldNames()));
        final String paMetastoreColNamesString = String.join(",", paMetastoreColumnNames);
        log.info("paMetastoreColumnNames: {}", paMetastoreColNamesString);

        if (paMetastoreColumnNames.size() != reserveColumnNames.size()) {
            throw new IllegalStateException("Number of columns in pa and reserve is different! " +
                    "There are : " + paMetastoreColumnNames.size() + " columns in pa " +
                    "and " + reserveColumnNames.size() + " columns in reserve");
        }
        SparkSQLUtil.checkTypesExactlyMatch(reserveStructType, paStructType);
    }

    private StructType addPartitionColumnsToSchema(List<String> partitions, StructType schema) {
        List<String> structFieldNames = asList(schema.fieldNames());
        List<StructField> structFields = new ArrayList<>(asList(schema.fields()));
        partitions.stream()
                .filter(partition -> !structFieldNames.contains(partition))
                .forEach(partition -> structFields.add(new StructField(partition, StringType, true, Metadata.empty())));

        return new StructType(structFields.toArray(new StructField[structFields.size()]));
    }

    private StructType getStructTypeWithoutPartition(List<String> partitions, StructType metastoreSchema) {
        StructField[] structFields = Stream.of(metastoreSchema.fields())
                .filter(sf -> !partitions.contains(sf.name()))
                .toArray(StructField[]::new);
        return new StructType(structFields);
    }

    private void executeSqlFromTemplate(Map<String, Object> data, String templateFileName) {
        Template template = getTemplate(templateFileName);

        StringWriter writer = new StringWriter();
        try {
            template.process(data, writer);
        } catch (IOException | TemplateException e) {
            throw new DatamartRuntimeException(e);
        }
        String sqlQuery = writer.toString();
        log.info("createPartitionedTable: {}", sqlQuery);
        context.sql(sqlQuery);
    }

    private void createPartitionedTable(StructType schema, String fullTableName, String partitioning) {
        String columns = SparkSQLUtil.joinedColumnNames(schema);
        FullTableName fullName = FullTableName.of(fullTableName);
        String externalLocation = null;
        if (!fullName.dbName().endsWith(STG_POSTFIX)) {
            String dbLocation = SparkSQLUtil.databaseLocation(context, fullName.dbName());
            externalLocation = dbLocation + "/" + fullName.tableName();
        }

        Map<String, Object> data = new HashMap<>();
        data.put("tableName", fullTableName);
        data.put("columns", columns);
        data.put("partitionBy", partitioning);
        data.put("extLocation", externalLocation);

        executeSqlFromTemplate(data, "create_table_partitioned.ftl");
    }

    public void insertOverwriteTable(String tableName, Dataset<Row> dataFrame) {
        dataFrameSaver.insertOverwriteTable(FullTableName.of(tableName), dataFrame);
    }

    public void insertOverwriteTable(String tableName, Dataset<Row> dataFrame, PartitionInfo partitionInfo) {
        dataFrameSaver.insertOverwriteTable(FullTableName.of(tableName), dataFrame, partitionInfo);
    }

    public void insertIntoTable(String tableName, Dataset<Row> dataFrame, PartitionInfo partitionInfo) {
        dataFrameSaver.insertIntoTable(FullTableName.of(tableName), dataFrame, partitionInfo);
    }

    public void saveReservingSnpTable(Dataset<Row> df,
                                      FullTableName reserveTableName,
                                      FullTableName paTableName,
                                      Set<String> partitionsToAdd) {
        final QueryExecution executionPLan = df.queryExecution();
        log.info("snapshot part {} queryExecution: {}", reserveTableName, executionPLan);
        saveReservingPartitionedTable(df, reserveTableName, paTableName, SNAPSHOT_PARTITIONING, true);

        if (!partitionsToAdd.isEmpty()) {
            String databaseLocation = SparkSQLUtil.databaseLocation(context, reserveTableName.dbName());
            String tablePath = databaseLocation + "/" + reserveTableName.tableName();
            MetastoreService metastoreService = new MetastoreService(context);
            for (String date : partitionsToAdd) {
                metastoreService.addPartition(reserveTableName, date, tablePath);
            }
        }
    }

    public void saveReservingHistTable(Dataset<Row> df, FullTableName historyReserveTableName, FullTableName histPartPaTableName) {
        final QueryExecution executionPlan = df.queryExecution();
        log.info("history part {} queryExecution: {}", historyReserveTableName, executionPlan);
        saveReservingPartitionedTable(df, historyReserveTableName, histPartPaTableName, HISTORY_PARTITIONING, true);
    }

    public void saveReservingPartitionedTable(Dataset<Row> df,
                                              FullTableName fullTableName,
                                              FullTableName paTableName,
                                              PartitionInfo partitionInfo,
                                              boolean repartition) {
        SparkSQLUtil.dropTable(context, fullTableName.fullTableName());
        log.info("Creating partitioned table '{}'", fullTableName);
        createPartitionedReserveTable(df.schema(), fullTableName.fullTableName(), paTableName.fullTableName(), partitionInfo);
        if (ENABLED_FLAG.equals(SysPropertyTool.getSystemProperty(SPARK_OPTION_PREFIX + SAVE_SALTING_PROPERTY))) {
            log.info("Trying to salt {}", fullTableName);
            if (partitionInfo.colNames().size() > 1) {
                throw new IllegalArgumentException("Cannot salt dataframe: partition info has more than 1 column");
            }
            df = salt(df, partitionInfo.colNames().get(0), SALT_ROWS, fullTableName.fullTableName() + SALT_TABLE_POSTFIX);
        } else if (repartition) {
            df = df.repartition(partitionInfo.columns());
        }
        log.info("Trying to insert into {}", fullTableName);
        insertOverwriteTable(fullTableName.fullTableName(), df, partitionInfo);
    }

    public void savePaTable(Dataset<Row> df, String paFullTableName, PartitionInfo partitionInfo) {
        SparkSQLUtil.dropTable(context, paFullTableName);

        if (partitionInfo != null) {
            log.info("Creating partitioned table {} with partitioning by '{}'", paFullTableName, partitionInfo.colNames());
            createPartitionedTable(df.schema(), paFullTableName, partitionInfo);
            log.info("Trying to insert into {}", paFullTableName);
            insertOverwriteTable(paFullTableName, df.repartition(partitionInfo.columns()), partitionInfo);
        } else {
            log.info("Creating non-partitioned table '{}'", paFullTableName);
            createTable(df.schema(), paFullTableName);
            log.info("insert to PA non-partitioned table {}", paFullTableName);
            insertOverwriteTable(paFullTableName, df);
        }
    }

    /**
     * Save directly to PA as partitioned table
     */
    public void savePaPartitionedTable(Dataset<Row> df, String paFullTableName, PartitionInfo partitionInfo) {
        log.info("save to PA {} with partitioning by {}", paFullTableName, String.join(",", partitionInfo.colNames()));
        SparkSQLUtil.dropTable(context, paFullTableName);
        log.info("Creating partitioned table '{}'", paFullTableName);
        createPartitionedTable(df.schema(), paFullTableName, partitionInfo);
        log.info("Trying to insert into {}", paFullTableName);
        Column[] partitionColumns = partitionInfo.colNames()
                .stream()
                .map(df::col)
                .toArray(Column[]::new);
        insertOverwriteTable(paFullTableName, df.repartition(partitionColumns), partitionInfo);
    }

    /**
     * Method create new repartitioned dataframe with calculated salt factor
     * * Available to use: partitioned table by 1 column
     * * (ex. Historical Datamarts hist and snp, Datamarts with PartitionedSavingStrategy)
     * partitionFactor - persist and broadcast:
     * * without broadcast - shuffle fails on summarize data in one bucket
     * * without saving - broadcast fails on lazy sending full dataframe
     * * with cache instead of save - caching probably fails on out of memmory
     * * with disk persist instead of save - persisted salt can be unexpectedly reused later
     * * and has no way to be unpersisted correctly
     */
    public Dataset<Row> salt(Dataset<Row> dataframe, String partitionColumnName, int rowsInPartition, String saltTableName) {
        Dataset<Row> partitionFactor = dataframe
                .repartition(200)
                .groupBy(partitionColumnName)
                .agg(count(lit(1)).as("count"))
                .select(col(partitionColumnName),
                        col("count").divide(lit(rowsInPartition)).cast(IntegerType)
                                .as("factor"));

        log.info("Saving dataframe salt as {}", saltTableName);
        partitionFactor = saveSalt(partitionFactor, saltTableName);
        partitionFactor = broadcast(partitionFactor);

        dataframe = dataframe
                .join(partitionFactor, partitionColumnName)
                .select(dataframe.col("*"),
                        rand().multiply(partitionFactor.col("factor")).cast(IntegerType).as(SALT_COLUMN));
        return dataframe
                .repartition(col(partitionColumnName), col(SALT_COLUMN))
                .drop(col(SALT_COLUMN));
    }

    private Dataset<Row> saveSalt(Dataset<Row> dataFrame, String saltTableName) {
        SparkSQLUtil.dropTable(context, saltTableName);
        createTable(dataFrame.schema(), saltTableName);
        insertOverwriteTable(saltTableName, dataFrame);
        return context.table(saltTableName);
    }
}
