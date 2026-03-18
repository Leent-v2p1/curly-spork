package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;

import java.util.Optional;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.*;

class DatamartNameBuilder {

    private static final String WRITABLE_SCHEMA_PREFIX = "custom_rb";

    private String resultSchema;
    private String resultTable;
    private Environment environment;
    private Optional<SourcePostfix> postfix;

    static DatamartNameBuilder builder() {
        return new DatamartNameBuilder();
    }

    DatamartNameBuilder setResultSchema(String resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }

    DatamartNameBuilder setResultTable(String resultTable) {
        this.resultTable = resultTable;
        return this;
    }

    DatamartNameBuilder setEnvironment(Environment environment) {
        this.environment = environment;
        return this;
    }

    DatamartNameBuilder setPostfix(Optional<SourcePostfix> postfix) {
        this.postfix = postfix;
        return this;
    }

    String fullTableName() {
        return resultSchema + "." + testEnvPrefix() + resultTable;
    }

    String resultName() {
        return testEnvPrefix() + resultTable;
    }

    String reserveFullTableName() {
        return resultSchema + stageSchemaPostfix() + "." + testEnvPrefix() + resultTable + RESERVE_POSTFIX;
    }

    String repartitionedFullTableName() {
        return resultSchema + stageSchemaPostfix() + "." + testEnvPrefix() + resultTable + REPARTITIONED_POSTFIX;
    }

    String backupFullTableName() {
        return resultSchema + stageSchemaPostfix() + "." + testEnvPrefix() + resultTable + BACKUP_POSTFIX;
    }

    String kafkaFullTableName() {
        return resultSchema + stageSchemaPostfix() + "." + testEnvPrefix() + resultTable + KAFKA_SAVER_POSTFIX;
    }

    String histBackupFullTableName() {
        return resultSchema + stageSchemaPostfix() + "." + testEnvPrefix() + resultTable + HIST_POSTFIX + BACKUP_POSTFIX;
    }

    String stage(String tableName) {
        return resultSchema + stageSchemaPostfix() + "." + testEnvPrefix() + tableName + sourceInstancePostfix();
    }

    String target(String tableName) {
        return resultSchema + "." + testEnvPrefix() + tableName + sourceInstancePostfix();
    }

    String stageSaveTemp(String tempTableName) {
        return resultSchema + stageSchemaPostfix() + "." + testEnvPrefix() + resultTable + "_" + tempTableName + sourceInstancePostfix();
    }

    String sourceStageTable(String schema, String tableName) {
        return schema + "." + testEnvPrefixForFullNames(schema, tableName) + tableName;
    }

    /**
     * Some datamarts have multiple instance, so table name should have additional postfix. Example: sbol_oper_IKFL2
     */
    private String sourceInstancePostfix() {
        return postfix.map(value -> "_" + value.getPostfix()).orElse("");
    }

    /**
     * Table in test environment starts with prefix 'test'. Example: custom_rb_card.TEST_card
     */
    private String testEnvPrefix() {
        return environment.isTestEnvironment() ? TEST_TABLE_PREFIX : "";
    }

    /**
     * Stage schema ends with postfix '_stg'. Example custom_rb_card_STG
     * If schema already ends with '_stg', then schema goes without changes
     */
    private String stageSchemaPostfix() {
        return resultSchema.endsWith(STG_POSTFIX) ? "" : STG_POSTFIX;
    }

    private String testEnvPrefixForFullNames(String resultTable, String schema) {
        return (environment.isTestEnvironment()
                && !resultTable.startsWith(TEST_TABLE_PREFIX)
                && schema.startsWith(WRITABLE_SCHEMA_PREFIX))
                ? TEST_TABLE_PREFIX
                : "";
    }
}
