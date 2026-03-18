package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.util.Objects;
import java.util.Optional;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.*;

public class DatamartNaming {

    private static final Logger log = LoggerFactory.getLogger(DatamartNaming.class);

    private final String sourceSchema;
    private final String resultSchema;
    private final DatamartNameBuilder baseNameBuilder;

    public DatamartNaming(String sourceSchema, String resultSchema, String resultTable, Environment environment) {
        this(environment, sourceSchema, resultSchema, resultTable, Optional.empty());
    }

    public DatamartNaming(Environment environment,
                          String sourceSchema,
                          String resultSchema,
                          String resultTable,
                          Optional<SourcePostfix> sourcePostfix) {
        String userName = SysPropertyTool.getSystemProperty("spark.user.name", "user");
        String clusterLetter = (userName.startsWith("u_arnsdprozn")) ? "p" : "u";
        this.sourceSchema = (sourceSchema != null) ? String.format(sourceSchema, clusterLetter) : null;
        this.resultSchema = resultSchema;
        this.baseNameBuilder = DatamartNameBuilder.builder()
                .setEnvironment(environment)
                .setResultSchema(resultSchema)
                .setResultTable(resultTable)
                .setPostfix(sourcePostfix);
    }

    public String resultSchema() {
        logValue("resultSchema", resultSchema);
        return resultSchema;
    }

    public String resultTable() {
        String resultTableName = baseNameBuilder.resultName();
        logValue("resultTable", resultTableName);
        return resultTableName;
    }

    public String sourceSchema() {
        logValue("sourceSchema", sourceSchema);
        return sourceSchema;
    }

    public String fullTableName() {
        String fullTableName = baseNameBuilder.fullTableName();
        logValue("fullTableName", fullTableName);
        return fullTableName;
    }

    public String historyFullTableName() {
        return fullTableName() + HIST_POSTFIX;
    }

    public String historyReserveFullTableName() {
        return reserveFullTableName() + HIST_POSTFIX;
    }

    public String reserveFullTableName() {
        String needMove = SysPropertyTool.getSystemProperty("spark.needMove", "false");
        String reserveFullTableName = baseNameBuilder.reserveFullTableName();
        if (needMove.equals("true")) {
            reserveFullTableName = sourceSchema + "_stg" + "." + resultTable() + RESERVE_POSTFIX;
        }
        logValue("reserveFullTableName", reserveFullTableName);
        return reserveFullTableName;
    }

    public String repartitionedFullTableName() {
        String repartitionedFullTableName = baseNameBuilder.repartitionedFullTableName();
        logValue("repartitionedFullTableName", repartitionedFullTableName);
        return repartitionedFullTableName;
    }

    public String backupFullTableName() {
        String backupFullTableName = baseNameBuilder.backupFullTableName();
        logValue("backupFullTableName", backupFullTableName);
        return backupFullTableName;
    }

    public String kafkaFullTableName() {
        String kafkaFullTableName = baseNameBuilder.kafkaFullTableName();
        logValue("kafkaFullTableName", kafkaFullTableName);
        return kafkaFullTableName;
    }

    public String historyBackupFullTableName() {
        String backupHistFullTableName = baseNameBuilder.histBackupFullTableName();
        logValue("backupHistFullTableName", backupHistFullTableName);
        return backupHistFullTableName;
    }

    public String histTableName() {
        return resultTable() + HIST_POSTFIX;
    }

    public String source(String name) {
        Objects.requireNonNull(sourceSchema, "sourceSchema is null. check datamart-properties sourceSchema field for " + fullTableName() + " is not empty");
        String source = sourceSchema + "." + name;
        logValue("source", source);
        return source;
    }

    public String sourceDiffTable(String name) {
        String diffSourceSchema = sourceSchema + DIFF_POSTFIX;
        String source = diffSourceSchema + "." + name;
        logValue("diffSource", source);
        return source;
    }

    public String sourceStageTable(String schema, String table) {
        String sourceStageTable = baseNameBuilder.sourceStageTable(schema, table);
        logValue("sourceStageTable", sourceStageTable);
        return sourceStageTable;
    }

    public String target(String name) {
        String target = baseNameBuilder.target(name);
        logValue("target", target);
        return target;
    }

    public String targetWithPostfix(String name) {
        String targetWithPostfix = baseNameBuilder.target(name);
        logValue("targetWithPostfix", targetWithPostfix);
        return targetWithPostfix;
    }

    public String stage(String tableName) {
        String stage = baseNameBuilder.stage(tableName);
        logValue("stage", stage);
        return stage;
    }

    /**
     * Name for tables saved with saveTemp.
     * Examples:
     * 1) for input: 'temp_name', in datmart 'sbol_oper' output: 'custom_rb_sbol_stg.sbol_oper_TEMP_NAME_ikfl2'
     * 2) for input: 'temp_name', in datmart 'card' output: 'custom_rb_card_stg.card_TEMP_NAME'
     */
    public String stageSaveTemp(String tableName) {
        String saveTempName = baseNameBuilder.stageSaveTemp(tableName);
        logValue("saveTempName", saveTempName);
        return saveTempName;
    }

    protected void logValue(String valueName, Object value) {
        log.info("naming.{} value = {}", valueName, value);
    }
}
