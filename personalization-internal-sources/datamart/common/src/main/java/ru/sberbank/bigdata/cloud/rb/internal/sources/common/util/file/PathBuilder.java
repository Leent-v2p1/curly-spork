package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.EnvironmentPathResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourceInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathBuilder {
    public static final String UNIX_PATH_DELIMETER = "/";
    protected static final String OOZIE_DIR = "oozie";
    protected static final String REPLICA_RECOVERY_DIR = "replica_recovery";
    protected final EnvironmentPathResolver envPathResolver;

    public PathBuilder(Environment environment) {
        this.envPathResolver = new EnvironmentPathResolver(environment);
    }

    /**
     * UNIX путь до папки в HDFS где находится таблица
     * Пример сгенерированного пути:
     * hdfs://clsklrozn/data/custom/rb/card/pa/card
     */
    public static String hdfsTablePath(String schema, String table, SparkSession context) {
        String databaseLocation = SparkSQLUtil.databaseLocation(context, schema);
        return databaseLocation + UNIX_PATH_DELIMETER + table;
    }

    /**
     * UNIX путь до папки содержащей workflow.xml
     * Пример сгенерированного пути для Environment.production и постфикса instanceName:
     * oozie-app/wf/custom/rb/way4/workflows/instanceName
     */
    public String resolveWorkflowPathForCtl(String schemaAlias, SourceInstance sourceModifier) {
        final String postfixForSource = sourceModifier.getWorkflowPathName();
        final Path pathToWorkflow = envPathResolver.resolveBaseAppPathFromOozieApp()
                .resolve(schemaAlias)
                .resolve("workflows")
                .resolve(postfixForSource);
        return AppPath.unixPath(pathToWorkflow);
    }

    /**
     * UNIX путь до потока построения по месячному периоду
     * Пример сгенерированного пути для Environment.production:
     * oozie-app/wf/custom/rb/way4/source-postfix-path/test-table/workflow.xml
     */
    public Path resolveMonthPeriodDatamart(FullTableName datamart, String sourcePostfixPath) {
        return envPathResolver.resolveBaseAppPathFromOozieApp()
                .resolve(getSchemaAlias(datamart))
                .resolve(sourcePostfixPath)
                .resolve(getValidTableName(datamart))
                .resolve("workflow.xml");
    }

    /**
     * UNIX путь до генерируемого в рантайме потока, строящего историческую витрину и реплику(при необходимости).
     * Пример сгенерированного пути для Environment.production_test:
     * /oozie-app/wf/custom/rb/production_test/way4/recovery-test-table/workflow.xml
     */
    public String resolveRuntimeWorkflowForRecovery(FullTableName datamart) {
        Path workflowPath = envPathResolver.resolveBaseAppPathFromOozieApp()
                .resolve(getSchemaAlias(datamart))
                .resolve("recovery-" + getValidTableName(datamart))
                .resolve("workflow.xml");
        return AppPath.unixPath(workflowPath);
    }

    /**
     * Путь в hdfs до директории содержащей поток восстанавливающий реплики на день одной витрины
     * Пример сгенерированного пути для Environment.uat_test:
     * \oozie-app\wf\custom\rb\\uat_test\way4\replica_recovery\test_table
     */
    public String resolveReplicaRecoveryDir(FullTableName datamart) {
        final Path path = envPathResolver.resolveBaseAppPathFromOozieApp()
                .resolve(getSchemaAlias(datamart))
                .resolve(REPLICA_RECOVERY_DIR)
                .resolve(datamart.tableName());
        return AppPath.unixPath(path);
    }

    /**
     * UNIX путь до потока построения витрины
     * Пример сгенерированного пути для Environment.production:
     * /oozie-app/wf/custom/rb/uat/way4/generated/create-test-table-wf
     */
    public String resolveGeneratedWorkflow(FullTableName datamart) {
        final Path path = resolveGeneratedWorkflow(Paths.get(""), datamart);
        return AppPath.unixPath(path);
    }

    /**
     * UNIX путь до временного хранилища статистик, которые должны быть опубликованы
     * Пример сгенерированного пути для Environment.production и витрины test-table c ctlLoadingId = 123:
     * /oozie-app/wf/custom/rb/production/way4/generated/create-test-table-wf/statistic_values_123.properties
     */
    public String resolveStatisticTempFile(FullTableName datamart, int ctlLoadingId) {
        final Path path = resolveGeneratedWorkflow(Paths.get(""), datamart)
                .resolve("statistic_values_" + ctlLoadingId + ".properties");
        return AppPath.unixPath(path);
    }

    /**
     * UNIX путь до временного хранилища id статистик, которые НЕ должны быть опубликованы
     * Пример сгенерированного пути для Environment.production и витрины test-table c ctlLoadingId = 456:
     * /oozie-app/wf/custom/rb/production/way4/generated/create-test-table-wf/statistic_disabled_456.properties
     */
    public String resolveDisabledStatisticTempFile(FullTableName datamart, int ctlLoadingId) {
        final Path path = resolveGeneratedWorkflow(Paths.get(""), datamart)
                .resolve("statistic_disabled_" + ctlLoadingId + ".properties");
        return AppPath.unixPath(path);
    }

    protected Path resolveGeneratedWorkflow(Path path, FullTableName datamart) {
        return path
                .resolve(envPathResolver.resolveGeneratedAppPath(getSchemaAlias(datamart)).toString())
                .resolve("create-" + getValidTableName(datamart) + "-wf");
    }

    /**
     * UNIX путь до файла csv католога
     * Пример сгенерированного пути для Environment.production:
     * /oozie-app/wf/custom/rb/way4/catalogs/catalog_name.csv
     */
    public String catalogPath(FullTableName catalogId) {
        final Path catalogsPath = envPathResolver.resolveBaseAppPathFromOozieApp()
                .resolve(getSchemaAlias(catalogId))
                .resolve("catalogs")
                .resolve(catalogId.tableName() + ".csv");
        return AppPath.unixPath(catalogsPath);
    }

    /**
     * UNIX путь до файла hql католога
     * Пример сгенерированного пути для Environment.production:
     * /oozie-app/wf/custom/rb/utils/dqcheck/way4/card-stop.hql
     */
    public String resolveEmergencyStopFile(FullTableName datamart) {
        final Path path = resolveDQCheckDir()
                .resolve(getSchemaAlias(datamart))
                .resolve(datamart.tableName() + "-stop.hql");
        return AppPath.unixPath(path);
    }

    /**
     * UNIX путь до файла hql католога
     * Пример сгенерированного пути для Environment.production:
     * /oozie-app/wf/custom/rb/way4/dqcheck/card-monitoring.hql
     */
    public String resolveMonitoringFile(FullTableName datamart) {
        final Path path = resolveDQCheckDir()
                .resolve(getSchemaAlias(datamart))
                .resolve(datamart.tableName() + "-monitoring.hql");
        return AppPath.unixPath(path);
    }

    /**
     * путь дo католога
     * Пример сгенерированного пути для Environment.production:
     * oozie-app/wf/custom/rb/utils/dqcheck/
     */
    public Path resolveDQCheckDir() {
        return envPathResolver.resolveBaseAppPathFromOozieApp()
                .resolve("utils")
                .resolve("dqcheck");
    }

    /**
     * путь дo католога c ключами для Kafka
     * Пример сгенерированного пути для Environment.production:
     * oozie-app/wf/custom/rb/utils/common/keystore
     */
    public String resolveKafkaKeystoreFile(String fileName) {
        final Path path = envPathResolver.resolveBaseAppPathFromOozieApp()
                .resolve("utils")
                .resolve("common")
                .resolve("keystore")
                .resolve(fileName);

        return AppPath.unixPath(path);
    }

    /**
     * UNIX путь до файла 'scheduler-pool.xml'
     * Пример сгенерированного пути для Environment.production:
     * /oozie-app/wf/custom/rb/production/utils/common/scheduler-pool.xml
     */
    public String schedulerPoolXmlPath() {
        final Path catalogsPath = envPathResolver
                .resolveBaseAppPathFromOozieApp()
                .resolve("utils")
                .resolve("common")
                .resolve("scheduler-pool.xml");
        return AppPath.unixPath(catalogsPath);
    }

    /**
     * UNIX путь до файла '<module>-datamart.jar'
     * Пример сгенерированного пути для модуля cod:
     * /oozie-app/wf/custom/rb/production/cod/cod-datamart.jar
     */
    public String jarPath(FullTableName tableName) {
        String schemaAlias = getSchemaAlias(tableName);
        final Path modulePath = envPathResolver
                .resolveBaseAppPathFromOozieApp()
                .resolve(schemaAlias)
                .resolve(schemaAlias + "-datamart.jar");
        return AppPath.unixPath(modulePath);
    }

    protected String getSchemaAlias(FullTableName datamart) {
        return SchemaAlias.of(datamart.dbName()).schemaAliasValue;
    }

    private String getValidTableName(FullTableName datamart) {
        return datamart.tableName().replace("_", "-");
    }
}
