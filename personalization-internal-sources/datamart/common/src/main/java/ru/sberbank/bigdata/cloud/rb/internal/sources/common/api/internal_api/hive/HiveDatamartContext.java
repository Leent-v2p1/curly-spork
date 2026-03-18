package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.Parametrizer;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.TableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class HiveDatamartContext implements DatamartContext {

    private static final Logger log = LoggerFactory.getLogger(HiveDatamartContext.class);
    public static final String TEMP_TABLE_POSTFIX = "self_tmp";

    private final SparkSession context;
    private final DatamartNaming naming;
    private final Environment environment;
    private final SchemaGrantsChecker grantsChecker;
    private final Parametrizer parametrizer;

    private HiveDatamartContext(SparkSession context,
                                DatamartNaming naming,
                                Environment environment,
                                SchemaGrantsChecker grantsChecker,
                                Parametrizer parametrizer) {
        this.context = context;
        this.naming = naming;
        this.environment = environment;
        this.grantsChecker = grantsChecker;
        this.parametrizer = parametrizer;
    }

    public static ContextBuilder builder(SparkSession context) {
        return new ContextBuilder(context);
    }

    private Dataset<Row> fullNamedTable(FullTableName fullTableName) {
        log.info("fullNamedTable = {}", fullTableName);
        grantsChecker.checkAccess(fullTableName);
        final Dataset<Row> table = context.table(fullTableName.fullTableName());

        if (environment.isTestEnvironment()) {
            log.warn("fake table retrieved with name = {}", fullTableName);
            return context().createDataFrame(emptyList(), table.schema());
        }
        final Dataset<Row> repartitionedDataframe = parametrizer.applyRepartitionAndCoalesce(table, fullTableName.tableName());
        log.info("actual table retrieved with name = {}", fullTableName);
        return repartitionedDataframe;
    }

    private Dataset<Row> fullNamedTable(String fullName) {
        return fullNamedTable(FullTableName.of(fullName));
    }


    @Override
    public Dataset<Row> sourceTable(TableName name) {
        FullTableName fullName = FullTableName.of(naming.source(name.tableName()));
        //SparkSQLUtil.recoverPartitions(context, fullName);
        return fullNamedTable(fullName);
    }

    @Override
    public Dataset<Row> sourceTable(FullTableName fullTableName) {
        return sourceTable(fullTableName.dbName(), fullTableName.tableName());
    }

    private Dataset<Row> sourceTable(String schema, String tableName) {
        FullTableName fullName = FullTableName.of(naming.sourceStageTable(schema, tableName));
        //SparkSQLUtil.recoverPartitions(context, fullName);
        return fullNamedTable(fullName);
    }

    @Override
    public Dataset<Row> sourceSparkTable(TableName name) {
        FullTableName fullName = FullTableName.of(naming.source(name.tableName()));
        return fullNamedTable(fullName);
    }

    @Override
    public Dataset<Row> sourceDiffTable(TableName name) {
        FullTableName fullName = FullTableName.of(naming.sourceDiffTable(name.tableName()));
        //SparkSQLUtil.recoverPartitions(context, fullName);
        return fullNamedTable(fullName);
    }

    @Override
    public Dataset<Row> targetTable(String name) {
        return fullNamedTable(naming.target(name));
    }

    @Override
    public Dataset<Row> targetTableWithPostfix(String name) {
        return fullNamedTable(naming.targetWithPostfix(name));
    }

    @Override
    public Dataset<Row> stageTable(String name) {
        return fullNamedTable(naming.stage(name));
    }

    @Override
    public Dataset<Row> stageTableWithPostfix(String name) {
        return fullNamedTable(naming.stageSaveTemp(name));
    }

    @Override
    public boolean exists() {
        return exists(naming.resultSchema(), naming.resultTable());
    }

    @Override
    public boolean exists(FullTableName fullTableName) {
        return exists(fullTableName.dbName(), fullTableName.tableName());
    }

    private boolean exists(String schemaName, String tableName) {
        return schemaTables(schemaName).contains(tableName);
    }

    @Override
    public List<String> schemaTables(String schema) {
        return asList(context.sqlContext().tableNames(schema));
    }

    @Override
    public Dataset<Row> loadExisting() {
        String fullName = naming.fullTableName();
        log.info("Loading existing table {}", fullName);
        return fullNamedTable(fullName);
    }

    @Override
    public SparkSession context() {
        return context;
    }

    public static class ContextBuilder {

        private final SparkSession context;
        private DatamartNaming naming;
        private SchemaGrantsChecker grantsChecker;
        private Environment environment;
        private Parametrizer parametrizer;

        private ContextBuilder(SparkSession context) {
            this.context = context;
        }

        public ContextBuilder naming(DatamartNaming naming) {
            if (this.naming != null) {
                log.warn("naming already exits, overwrite it");
            }
            this.naming = naming;
            return this;
        }

        public ContextBuilder grantsChecker(SchemaGrantsChecker grantsChecker) {
            this.grantsChecker = grantsChecker;
            return this;
        }

        public ContextBuilder environment(Environment environment) {
            this.environment = environment;
            return this;
        }

        public ContextBuilder parametrizer(Parametrizer parametrizer) {
            this.parametrizer = parametrizer;
            return this;
        }

        public HiveDatamartContext create() {
            return new HiveDatamartContext(context, naming, environment, grantsChecker, parametrizer);
        }
    }
}
