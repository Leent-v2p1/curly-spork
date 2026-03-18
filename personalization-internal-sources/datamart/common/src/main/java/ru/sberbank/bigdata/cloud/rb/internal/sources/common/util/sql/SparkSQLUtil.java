package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.DbName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.DbNameImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaNames.getReplicaNamesForMsckRepair;

/**
 * Функции для работы со Spark Util
 */
public class SparkSQLUtil {

    private static final Logger log = LoggerFactory.getLogger(SparkSQLUtil.class);

    /**
     * @return строку содержащую путь до схемы, выключая префикс указывающий на файловую систему('file:/', 'hdfs://')
     */
    public static String databaseLocation(SparkSession sqlContext, String schema) {
        final Dataset<Row> databaseMetadata = sqlContext.sql("DESCRIBE database " + schema);
        return databaseMetadata
                .where(databaseMetadata.col("database_description_item").equalTo("Location"))
                .select(databaseMetadata.col("database_description_value"))
                .first()
                .getString(0);
    }

    public static Optional<String> tableLocation(SparkSession sqlContext, String table) {
        final FullTableName fullTableName = FullTableName.of(table);
        final boolean exists = asList(sqlContext.sqlContext().tableNames(fullTableName.dbName())).contains(fullTableName.tableName()
                .toLowerCase());
        if (!exists) {
            return Optional.empty();
        }

        final Dataset<Row> tableMetadata = sqlContext.sql("DESC FORMATTED " + table)
                .where(col("col_name").equalTo("Location"))
                .select(col("data_type"));
        if (tableMetadata.count() == 0) {
            return Optional.empty();
        }

        final String tableLocation = tableMetadata.first().getString(0);
        return Optional.of(tableLocation);
    }

    public static boolean isTableExists(SparkSession sqlContext, FullTableName table) {
        return Arrays.stream(sqlContext.sqlContext().tableNames(table.dbName()))
                .anyMatch(tableInSchema -> tableInSchema.equals(table.tableName()));
    }

    public static boolean isSchemaExists(SparkSession sqlContext, String schema) {
        return sqlContext.sql("show databases")
                .collectAsList()
                .stream()
                .map(row -> row.getString(0))
                .anyMatch(s -> s.equals(schema));
    }

    public static Dataset<Row> unionAll(Dataset<Row>... frames) {
        return Stream.of(frames)
                .reduce(SparkSQLUtil::unionAll)
                .orElseThrow(() -> new IllegalArgumentException("Cannot merge " + frames.length + "data frames"));
    }

    public static Dataset<Row> unionAll(Dataset<Row> df1, Dataset<Row> df2) {
        List<String> df1Columns = asList(df1.columns());
        List<String> df2Columns = asList(df2.columns());

        final StructType schema = df1.schema();
        final StructType schema2 = df2.schema();
        log.info("SparkSQLUtil#unionAll \ndf1Columns = [{}], \ndf2Columns = [{}]", schema, schema2);
        checkTypes(df1, df2);

        Set<String> combinedColumns = new LinkedHashSet<>(df1Columns);
        combinedColumns.addAll(df2Columns);

        Dataset<Row> df1Ordered = df1.select(addNulls(df1Columns, combinedColumns));
        Dataset<Row> df2Ordered = df2.select(addNulls(df2Columns, combinedColumns));

        return df1Ordered.union(df2Ordered);
    }

    public static String joinedColumnNames(StructType schema) {
        return joinedColumnNamesWithFilter(schema, structField -> true);
    }

    public static String joinedColumnNamesWithFilter(StructType schema, Predicate<? super StructField> predicate) {
        return Stream.of(schema.fields())
                .filter(predicate)
                .map(structField -> "`" + structField.name() + "` " + structField.dataType().simpleString())
                .collect(joining(", "));
    }

    private static Column[] addNulls(List<String> columnsFromDf, Set<String> combinedColumns) {
        return combinedColumns.stream()
                .map(colName -> columnsFromDf.contains(colName) ? col(colName) : lit(null).as(colName))
                .toArray(Column[]::new);
    }

    public static String format(List<StructField> schema) {
        return schema.stream()
                .map(structField -> String.format("(%s,%s)", structField.name(), structField.dataType()))
                .collect(joining(",", "[", "]"));
    }

    public static String formatPartitions(List<PartitionInfo.Partition> partitions) {
        return partitions
                .stream()
                .map(s -> String.format("`%s` %s", s.name, s.type.typeName()))
                .collect(Collectors.joining(", "));
    }

    private static void checkTypes(Dataset<Row> df1, Dataset<Row> df2) {

        Map<String, StructField> df2NameTypeMap = Arrays.stream(df2.schema().fields()).collect(toMap(StructField::name, identity()));

        List<StructField> df1NotMatchedFields = Arrays.stream(df1.schema().fields())
                .filter(structField -> df2NameTypeMap.containsKey(structField.name()))
                .filter(structField -> !df2NameTypeMap.get(structField.name()).dataType().equals(structField.dataType()))
                .collect(toList());

        if (!df1NotMatchedFields.isEmpty()) {
            List<StructField> df2NotMatchedFields = df1NotMatchedFields.stream()
                    .map(structField -> df2NameTypeMap.get(structField.name()))
                    .collect(toList());
            throw new DatamartRuntimeException("Column types doesn't match, not matched columns: \n" +
                    "df1Columns = " + format(df1NotMatchedFields) + ", \n" +
                    "df2Columns = " + format(df2NotMatchedFields));
        }
    }

    public static void checkTypesExactlyMatch(StructType schema1, StructType schema2) {
        checkTypesExactlyMatch(schema1, "df1Columns", schema2, "df2Columns");
    }

    public static void checkTypesExactlyMatch(StructType schema1, String schema1Name, StructType schema2, String schema2Name) {

        class Types {
            final String firstType;
            final String secondType;

            public Types(String firstType, String secondType) {
                this.firstType = firstType;
                this.secondType = secondType;
            }
        }

        /*
        Разбивает Types на 3 группы и сортирует их в следующем порядке: первыми идут колонки, присутствующие в обоих датафреймах,
        затем колонки, которые отсутствуют в первом датафрейме, но есть во втором,
        и в конце колонки, которые отсутствуют во втором датафрейма, но есть в первом,
         */
        final Comparator<Types> typesComparator = (el1, el2) -> {
            if ((!el1.firstType.equals("-") && !el1.secondType.equals("-") && !el2.firstType.equals("-") && !el2.secondType.equals("-"))
                    || (el1.firstType.equals("-") && el2.firstType.equals("-"))
                    || (el1.secondType.equals("-") && el2.secondType.equals("-"))) {
                return 0;
            } else if ((!el1.firstType.equals("-") && !el1.secondType.equals("-")) || el2.secondType.equals("-")) {
                return -1;
            } else {
                return 1;
            }
        };

        final List<StructField> diff12 = schemaDiff(schema1, schema2);
        final List<StructField> diff21 = schemaDiff(schema2, schema1);

        final Map<String, Types> typesMap = diff12.stream().collect(toMap(StructField::name, f -> new Types(f.dataType().typeName(), "-")));
        diff21.forEach(row -> typesMap.merge(row.name(), new Types("-", row.dataType().typeName()), (oldType, newType) -> new Types(oldType.firstType, newType.secondType)));

        final List<Row> typeRows = typesMap
                .entrySet()
                .stream()
                .sorted(Map.Entry.<String, Types>comparingByValue(typesComparator)
                        .thenComparing(Map.Entry.comparingByKey(Comparator.naturalOrder())))
                .map(entry -> RowFactory.create(entry.getKey(), entry.getValue().firstType, entry.getValue().secondType))
                .collect(toList());

        final String mismatches = DatasetFormatter.formatDatasetMsg(asList("Column Name", "Type from " + schema1Name, "Type from " + schema2Name), typeRows);

        if (!diff12.isEmpty() || !diff21.isEmpty()) {
            throw new DatamartRuntimeException("Columns don't match, not matched columns: \n" +
                    mismatches);
        }
    }

    public static void checkTypeOneInteration(StructType schema1, StructType schema2) {
        final List<StructField> diff12 = schemaDiff(schema1, schema2);
        if (!diff12.isEmpty()) {
            throw new DatamartRuntimeException("Columns don't match, not matched columns: \n" +
                    "df1Columns = " + format(diff12));
        }
    }

    public static List<StructField> schemaDiff(StructType schema1, StructType schema2) {
        Map<String, StructField> df2NameTypeMap = Arrays.stream(schema2.fields())
                .collect(toMap(SparkSQLUtil::getLowerCaseName, identity()));

        return Arrays.stream(schema1.fields())
                .filter((structField -> !df2NameTypeMap.containsKey(getLowerCaseName(structField)) || !df2NameTypeMap.get(getLowerCaseName(structField))
                        .dataType()
                        .simpleString()
                        .equals(structField.dataType().simpleString())))
                .collect(toList());
    }

    private static String getLowerCaseName(StructField structField) {
        return structField.name().toLowerCase();
    }

    /**
     * Joins two skewed dataframes
     *
     * @param df1 dataframe
     * @param df2 skewed dataframe
     * @param id1ColName first dataframe's column name
     * @param id2ColName second dataframe's skewed column name
     * @param skewedIdArr keys that define skewed rows
     */
    public static Dataset<Row> skewJoin(Dataset<Row> df1,
                                        Dataset<Row> df2,
                                        String id1ColName,
                                        String id2ColName,
                                        Column joinCondition,
                                        String joinType,
                                        Object... skewedIdArr) {
        List<Dataset<Row>> skewedDfList = new ArrayList<>();

        for (Object skewedId : skewedIdArr) {
            final Dataset<Row> df1Skewed = df1.where(df1.col(id1ColName).equalTo(lit(skewedId)));
            final Dataset<Row> df2Skewed = df2.where(df2.col(id2ColName).equalTo(lit(skewedId)));
            skewedDfList.add(df1Skewed.join(broadcast(df2Skewed), joinCondition, joinType));
        }

        Column filter1 = lit(true);
        Column filter2 = lit(true);
        for (Object skewedId : skewedIdArr) {
            filter1 = filter1.and(df1.col(id1ColName).notEqual(skewedId));
            filter2 = filter2.and(df2.col(id2ColName).notEqual(skewedId));
        }

        final Dataset<Row> df1NotSkewed = df1.where(filter1);
        final Dataset<Row> df2NotSkewed = df2.where(filter2);
        final Dataset<Row> joinNotSkewed = df1NotSkewed.join(df2NotSkewed, joinCondition, joinType);
        Dataset<Row> result = joinNotSkewed;
        for (Dataset<Row> skewedDf : skewedDfList) {
            result = result.union(skewedDf);
        }
        return result;
    }

    /**
     * Splits join buckets of left dataframe by modifying the joining key(bucket_column)
     * And replicate right dataframe for each new joining key(bucket_column)
     *
     * @param leftDf big table
     * @param rightDf smaller table. it would be increased by @param maxRange times, so needs to be careful with it
     * @param sqlContext context for utility reasons
     * @param joinCondition default join condition
     * @param joinType only inner / left
     * @param replicationFactor number of buckets
     * Example: #joinCondition by col('key') with #replicationFactor 2
     * key1 -> key1_bucket0
     * key1 -> key1_bucket1
     * key1 -> key1_bucket0
     * key2 -> key2_bucket0
     * key2 -> key2_bucket1
     * key3 -> key3_bucket0
     */
    public static Dataset<Row> saltedJoin(Dataset<Row> leftDf,
                                          Dataset<Row> rightDf,
                                          SparkSession sqlContext,
                                          Column joinCondition,
                                          String joinType,
                                          int replicationFactor) {
        leftDf = leftDf.withColumn("bucket_column", functions.monotonicallyIncreasingId().mod(replicationFactor));
        final List<Integer> values = IntStream.range(0, replicationFactor).boxed().collect(toList());
        final Dataset<Row> bucketValue = DataFrameUtils.createOneFieldDf(sqlContext,
                new StructType().add("bucket_column_2", IntegerType),
                values);
        rightDf = rightDf.crossJoin(bucketValue);//cross join to generate all possible key pairs

        return leftDf.join(rightDf,
                joinCondition.and(leftDf.col("bucket_column").equalTo(rightDf.col("bucket_column_2"))), joinType)
                .drop("bucket_column")
                .drop("bucket_column_2");
    }

    /**
     * Splits join buckets of left dataframe by modifying the joining key(bucket_column)
     * And replicate right dataframe for each new joining key(bucket_column)
     *
     * @param leftDf big table
     * @param rightDf smaller table. it would be increased by @param maxRange times, so needs to be careful with it
     * @param leftCol first dataframe's column name
     * @param rightCol second dataframe's skewed column name
     * @param sqlContext context for utility reasons
     * @param joinCondition default join condition
     * @param joinType only inner / left
     * @param replicationFactor number of buckets
     * Example: #joinCondition=col('key'), #replicationFactor=2, #leftSkewedColumn="key", #freqLimit=0.5(more than half of all values)
     * key1 -> key1_bucket0
     * key1 -> key1_bucket1
     * key1 -> key1_bucket0
     * key2 -> key2_bucket0
     * key2 -> key2_bucket0
     * key3 -> key3_bucket0
     */
    public static Dataset<Row> filteredSaltedJoin(Dataset<Row> leftDf,
                                                  Dataset<Row> rightDf,
                                                  String leftCol,
                                                  String rightCol,
                                                  SparkSession sqlContext,
                                                  Column joinCondition,
                                                  String joinType,
                                                  int replicationFactor,
                                                  Object... skewedIdArr) {
        final List<Integer> values = IntStream.range(0, replicationFactor).boxed().collect(toList());
        final Dataset<Row> bucketValue = broadcast(
                DataFrameUtils.createOneFieldDf(sqlContext,
                        new StructType().add("bucket_column_2", IntegerType),
                        values));
        final Broadcast<Object[]> broadcastValues = JavaSparkContext
                .fromSparkContext(sqlContext.sparkContext())
                .broadcast(skewedIdArr);

        leftDf = leftDf.withColumn("bucket_column",
                when(leftDf.col(leftCol).isin(broadcastValues.value()),
                        functions.monotonicallyIncreasingId().mod(replicationFactor))
                        .otherwise(lit(0)));

        rightDf = rightDf
                .join(bucketValue, rightDf.col(rightCol).isin(broadcastValues.value())
                        .or(bucketValue.col("bucket_column_2").equalTo(lit(0))));
        joinCondition = joinCondition.and(leftDf.col("bucket_column").equalTo(rightDf.col("bucket_column_2")));
        return leftDf
                .join(rightDf, joinCondition, joinType)
                .drop(col("bucket_column"))
                .drop(col("bucket_column_2"));
    }

    /**
     * Возвращает значение, которое в Оракле можно было бы получить вызовом функции
     * max(valueName) keep(dense_rank last order by orderByColumnName),
     * то есть найти максимальное значение в колонке orderByColumnName и из всех соответствующих ему значений
     * колонки valueName взять максимум
     *
     * @param data - dataFrame, из которого получаем данные
     * @param orderByColumnName - колонка, по которой идет внутренняя сортировка
     * @param valueName - колонка, максимальное значение которой нужно вернуть
     *
     * @return - dataFrame, содержащий одну колонку и одну строку - максимальное значение valueName при максимальном значении orderByColumnName
     */
    public static Dataset<Row> keepDenseRankMaxLast(Dataset<Row> data, String orderByColumnName, String valueName) {
        Object maxDate = data.agg(functions.max(orderByColumnName))
                .select("max(" + orderByColumnName + ")").first().get(0);
        Dataset<Row> result = data.where(col(orderByColumnName).equalTo(maxDate));
        return result.agg(functions.max(valueName).as(valueName));
    }

    /**
     * @return List of partitions in format ["key1=value1/key2=value2", "key1=value3/key2=value4"]
     */
    public static List<String> getPartitionList(SparkSession context, String tableName) {
        List<String> partitions = catchPartitionNotFound(
                () -> getPartitionStream(context, tableName).collect(toList()),
                Collections::emptyList);
        log.info("got partitions for table '{}': {}", tableName, partitions);
        return partitions;
    }

    public static List<String> getPartitionList(SparkSession context, FullTableName fullTableName) {
        return getPartitionList(context, fullTableName.fullTableName());
    }

    private static <T> T catchPartitionNotFound(Supplier<T> action, Supplier<T> catchAction) {
        try {
            return action.get();
        } catch (Exception e) {
            // "SHOW PARTITIONS" throws checked exception, if table isn't partitioned,
            if (e.getMessage().contains("is not partitioned")) {
                return catchAction.get();
            } else {
                throw e;
            }
        }
    }

    private static Stream<String> getPartitionStream(SparkSession sqlContext, String tableName) {
        return sqlContext
                .sql("SHOW PARTITIONS " + tableName)
                .collectAsList()
                .stream()
                .map(part -> part.getString(0));
    }

    public static List<PartitionInfo> getPartitions(SparkSession sqlContext, FullTableName fullTableName) {
        Optional<PartitionInfo> partInfo = getPartitionInfo(sqlContext, fullTableName.fullTableName());
        if (!partInfo.isPresent()) {
            return Collections.emptyList();
        }

        return getPartitionStream(sqlContext, fullTableName.fullTableName())
                .map(subPart ->
                        Arrays.stream(subPart.split("/"))
                                .map(partition -> partition.split("="))
                                .map(namePath -> {
                                    String name = namePath[0];
                                    DataType type = partInfo.get()
                                            .getPartition(name)
                                            .map(p -> p.type)
                                            .orElse(StringType);
                                    String value = namePath[1];
                                    return new PartitionInfo.Partition(name, type, value);
                                })
                                .collect(toList())
                )
                .map(partitionList -> PartitionInfo.nonDynamic().add(partitionList).create())
                .collect(toList());
    }

    /**
     * @return Dynamic PartitionInfo of partitioned columns
     */
    public static Optional<PartitionInfo> getPartitionInfo(SparkSession context, String fullTableName) {
        String createTable = getDDL(context, fullTableName).toLowerCase();

        Pattern partitionedByRegexp = Pattern.compile("partitioned\\s+by\\s+\\(([^)]+)");
        final Matcher matcher = partitionedByRegexp.matcher(createTable);
        if (!matcher.find()) {
            log.warn("Table {} not partitioned", fullTableName);
            return Optional.empty();
        }
        Pattern partitionedColumnRegexp = Pattern.compile("`(\\w+)`");
        StructType struct = context.table(fullTableName).schema();
        log.warn("Struct of fullTableName={}, is {}", fullTableName, struct);
        log.warn("Create table statement is: {}", createTable);
        String partitioning = matcher.group(1);
        log.warn("Partitiong is {}", partitioning);
        PartitionInfo.PartitionInfoDynamicBuilder partInfo = PartitionInfo.dynamic();
        Arrays.stream(partitioning.split(","))
                .map(partitionedColumnRegexp::matcher)
                .filter(Matcher::find)
                .map(m -> m.group(1))
                .forEach(part -> partInfo.add(part, struct.apply(part.toLowerCase()).dataType()));
        String forDebug = Arrays.stream(partitioning.split(","))
                .map(partitionedColumnRegexp::matcher)
                .filter(Matcher::find)
                .map(m -> m.group(1))
                .collect(Collectors.joining(","));
        log.warn("partitions: {} and {}", partInfo, forDebug);
        return Optional.of(partInfo.create());
    }

    public static void dropTable(SparkSession sqlContext, String table) {
        log.info("Removing hdfs files of table {}", table);
        tableLocation(sqlContext, table).ifPresent(HDFSHelper::deleteDirectory);

        log.info("Executing drop table of {}", table);
        final String dropTableSql = String.format("drop table if exists %s", table);
        log.info(dropTableSql);
        log.info("SQL Context: {}", sqlContext);
        sqlContext.sql(dropTableSql);
        log.info("Logs after dropping table.");
    }

    /**
     * Drop table from metastore
     * For external tables: files stay untouched
     */
    public static void dropAndInvalidate(SparkSession context, FullTableName table) {
        dropTable(context, table.fullTableName());
        if (isTableExists(context, table)) {
            context.catalog().refreshTable(table.fullTableName());
        }
    }

    public static boolean isPartitioned(SparkSession context, String tableName) {
        return getDDL(context, tableName).contains("PARTITIONED BY");
    }

    public static boolean isExternal(SparkSession context, String tableName) {
        return getDDL(context, tableName).contains("EXTERNAL");
    }

    public static String getDDL(SparkSession context, String tableName) {
        String hdpType = SysPropertyTool.getSystemProperty("spark.hdpType", "hdp");
        if (hdpType.equals("sdp")) {
            log.info("HDP TYPE SDP !!!!!!!!");
            return context.sql("SHOW CREATE TABLE " + tableName + " AS SERDE")
                    .select("createtab_stmt")
                    .first()
                    .getString(0);
        } else {
            return context.sql("SHOW CREATE TABLE " + tableName)
                    .select("createtab_stmt")
                    .first()
                    .getString(0);
        }

    }

    /**
     * @param dataframe which produces column array
     *
     * @return Array of dataframe columns in Column format
     */
    public static Column[] getColumns(Dataset<Row> dataframe) {
        return getColumns(dataframe.schema());
    }

    public static Column[] getColumns(StructType schema) {
        return Arrays.stream(schema.fields()).map(sf -> col(sf.name())).toArray(Column[]::new);
    }

    /**
     * Метод приводит датафрейм к определенной структуре
     *
     * @param df исходный датасет
     * @param schema схема, к которой будет преобразован датасет
     */
    public static Dataset<Row> matchDfToSchema(Dataset<Row> df, StructType schema) {

        List<List<Object>> newColsWithTypes =
                Arrays.stream(schema.fields())
                        .map(c -> Arrays.asList(c.name(), c.dataType()))
                        .collect(toList());

        log.info("Matching existing dataframe to schema with new columns: {}",
                newColsWithTypes.stream().map(c -> String.format("(%s, %s)", c.get(0).toString(), c.get(1).toString()))
                        .collect(joining(", ")));

        return  df.select(newColsWithTypes.stream()
                .map((c -> col((String) c.get(0)).cast((DataType) c.get(1))))
                .toArray(Column[]::new));
    }

    /**
     * Метод вычисляет результат агрегатной функции (например, min или max) для заданной колонки.
     *
     * @param table датасет с колонкой для агрегации
     * @param field колонка, по которой вычисляется агрегат
     * @param aggrFunction агрегатная функция
     *
     * @return переданный датасет 'recount' с заполненными пропусками
     */
    public static <T> T getAggColumnValue(Dataset<Row> table, String field, Function<Column, Column> aggrFunction) {
        return table
                .select(aggrFunction.apply(table.col(field)).as("aggr_value"))
                .collectAsList()
                .get(0)
                .getAs("aggr_value");
    }

    public static long getMaxLoadingId(Dataset<Row> table) {
        return getAggColumnValue(table, "ctl_loading", functions::max);
    }

    public static boolean isEmpty(Dataset<Row> dataFrame) {
        return dataFrame.limit(1).count() == 0;
    }

    public static Column lpad0(Column str, int len) {
        return lpad(str, len, "0");
    }

    public static void copyTable(SparkSession context, String fromTable, String toTable) {
        log.info("Creating table ({}) from {}", toTable, fromTable);
        SparkSQLUtil.dropTable(context, toTable);
        new TableSaveHelper(context).createCopyTable(fromTable, toTable);
        copyFiles(context, fromTable, toTable);
        if (isPartitioned(context, toTable)) {
            context.sql("MSCK REPAIR TABLE " + toTable);
        }
    }

    /**
     * @param context - SparkSession
     * @param df - input DataSet
     * @param columnName - name of column to add increment
     * @param startIdFrom - value for initial increment (included)
     *
     * @return the sequential number of a row starting from startIdFrom
     *
     * Example:
     * 1. columnName = od_id, startIdFrom = 567; df:
     * |merchant_id|
     * |         50|
     * |         60|
     * |         70|
     *
     * Returns:
     * od_id|merchant_id|
     * 568  |         50|
     * 569  |         60|
     * 570  |         70|
     */
    public static Dataset<Row> addIncrementId(SparkSession context, Dataset<Row> df, String columnName, long startIdFrom) {
        StructType resultSchema = df.schema().add(columnName, LongType);
        final JavaRDD<Row> rdd = df
                .javaRDD()
                .zipWithIndex()
                .map((org.apache.spark.api.java.function.Function<Tuple2<Row, Long>, Row>) rowAndIndex -> {
                    final Row prevRow = rowAndIndex._1;
                    final int prevRowSize = prevRow.size();
                    Object[] newRow = new Object[prevRowSize + 1];
                    newRow[prevRowSize] = rowAndIndex._2 + startIdFrom;
                    for (int i = 0; i < prevRowSize; i++) {
                        newRow[i] = prevRow.get(i);
                    }
                    return RowFactory.create(newRow);
                });
        return context.createDataFrame(rdd, resultSchema);
    }

    /**
     * Default catalog().refreshTable() method would cause NoSuchTableException if table doesn't exist, this method prevents it
     */
    public static void safeTableRefresh(SparkSession sparkSession, FullTableName table) {
        if (isSchemaExists(sparkSession, table.dbName()) && isTableExists(sparkSession, table)) {
            sparkSession.catalog().refreshTable(table.fullTableName());
        }
    }

    private static void copyFiles(SparkSession context, String sourceTable, String targetTable) {
        final FullTableName source = FullTableName.of(sourceTable);
        final FullTableName target = FullTableName.of(targetTable);
        final Path sourcePath = new Path(PathBuilder.hdfsTablePath(source.dbName(), source.tableName(), context));
        final Path targetPath = new Path(PathBuilder.hdfsTablePath(target.dbName(), target.tableName(), context));
        log.info("Coping files from: {} to: {}", sourcePath, targetPath);
        HDFSHelper.copyFiles(sourcePath, targetPath);
    }

    public static void recoverPartitions(SparkSession context, FullTableName fullTableName) {
        final List<DbName> replicaNamesForMsck = getReplicaNamesForMsckRepair();
        final DbNameImpl dbName = new DbNameImpl(fullTableName.dbName());
        if (replicaNamesForMsck.contains(dbName) && isPartitioned(context, fullTableName.fullTableName())) {
            log.info("Updating metadata about partitions for table: {}", fullTableName);
            context.sql("USE " + fullTableName.dbName());
            context.sql("MSCK REPAIR TABLE " + fullTableName.tableName());
            context.sql("USE DEFAULT");
        }
    }

    public static boolean isRowValueNull(Row row, String col) {
        return row.getAs(col) == null || row.getAs(col).equals("null");
    }
}
