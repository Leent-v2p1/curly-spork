package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.ddl;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * command to run:
 * spark-submit --class ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.ddl.DDLExtractor --conf "spark.driver.extraJavaOptions=-Dschema.output=/tmp/schemas -Dschema.list=custom_rb_card_stg -Dhive.metastore.uris=thrift://hw2288-02.cloud.dev.df.sbrf.ru:9083" --master yarn --deploy-mode cluster personalization.jar
 *
 * -Dschema.output=/tmp/schemas – путь до папки в hdfs, в которую будут записаны файлы схем:
 * -Dschema.list=custom_rb_card_stg  список схем через SCHEMA_NAME_SEPARATOR, либо просто одна схема
 *
 * Для некоторых схем требуются дополнительные параметры спарка, которые нужно добавить в extraJavaOptions при запуске.
 * В дженкинс-джобе их можно передать через параметр "Spark_Parameter".
 * Параметры можно искать в datamart-properties по нужной sourceSchema. Но в целом, помогают следующие параметры:
 * - spark.sql.hive.convertMetastoreParquet=false
 * - spark.sql.parquet.binaryAsString=true
 * - spark.hadoop.hive.mapred.supports.subdirectories=true
 * - spark.hadoop.mapred.input.dir.recursive=true
 */
public class DDLExtractor {

    private static final String SCHEMA_NAME_SEPARATOR = ",";
    private static final String SCHEMAS_PROPERTY_KEY = "schema.list";
    private static final String OUTPUT_PROPERTY_KEY = "schema.output";
    private static final String JSON_FILE_EXTENSION = ".json";

    private final Path rootPath;
    private final SparkSession context;
    private final SchemaSerializer serializer;
    private final String fileExtension;

    public DDLExtractor(Path rootPath, SparkSession context, SchemaSerializer serializer, String fileExtension) {
        this.rootPath = rootPath;
        this.context = context;
        this.serializer = serializer;
        this.fileExtension = fileExtension;
    }

    public void invokeFor(List<String> schemasToSerialize) {
        HDFSHelper.deleteDirectory(rootPath.toAbsolutePath().toString());
        schemasToSerialize.forEach(this::writeSerializedSchema);
    }

    protected void writeSerializedSchema(String schema) {
        final String[] tablesInSchema = context.sqlContext().tableNames(schema);
        final String schemaContent = Stream.of(tablesInSchema)
                .map(table -> FullTableName.of(schema, table))
                .map(this::serializeTableSchema)
                .collect(Collectors.joining("\n"));

        final Path path = rootPath.resolve(schema + fileExtension).toAbsolutePath();
        HDFSHelper.rewriteFile(schemaContent, path);
    }

    private String serializeTableSchema(FullTableName fullName) {
        final StructType tableSchema = context.table(fullName.toString()).schema();
        return serializer.serialize(fullName.tableName(), tableSchema);
    }

    public static void main(String[] args) {
        final String schemas = SysPropertyTool.safeSystemProperty(SCHEMAS_PROPERTY_KEY);
        final List<String> schemaList = Arrays.asList(schemas.split(SCHEMA_NAME_SEPARATOR));
        final String schemaDir = SysPropertyTool.safeSystemProperty(OUTPUT_PROPERTY_KEY);
        final SparkSession sparkSession = SparkSession.builder()
                .appName("schema-serialization-job")
                .getOrCreate();
        new DDLExtractor(Paths.get(schemaDir), sparkSession, new JsonSchemaSerializer(), JSON_FILE_EXTENSION).invokeFor(schemaList);
    }
}
