package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.sql.Timestamp;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.ENABLED_FLAG;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.SPARK_OPTION_PREFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.STAT_TABLE_UPDATE_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils.row;

/**
 * StatTableWriter предназначен для загрузки информации по сущностям витрин в таблицу wf_od_stats
 */
public class StatTableWriter {

    private static final Logger log = LoggerFactory.getLogger(StatTableWriter.class);
    static final StructType OD_STATS_SCHEMA = new StructType()
            .add("sync_ctl_loading_id", LongType)
            .add("sync_dttm", TimestampType)
            .add("ctl_loading_id", LongType)
            .add("wf_name", StringType)
            .add("entity_id", LongType)
            .add("stat_id", LongType)
            .add("stat_value", StringType)
            .add("profile_name", StringType);

    private static final String STAT_TABLE = "wf_od_stats";

    private Timestamp loadingTime;
    private DatamartContext dc;
    private SparkSession sqlContext;
    private long loadingId;
    private long entityId;
    private FullTableName datamartTable;
    private String profile;

    public StatTableWriter(DatamartContext datamartContext,
                           SparkSession sqlContext,
                           Integer loadingId,
                           String profile,
                           String datamartSchema,
                           String datamartTable,
                           Integer entityId,
                           Timestamp loadingTime
    ) {
        this.dc = datamartContext;
        this.sqlContext = sqlContext;
        this.loadingId = loadingId;
        this.profile = profile;
        this.datamartTable = FullTableName.of(datamartSchema, datamartTable);
        this.entityId = entityId;
        this.loadingTime = loadingTime;
    }

    public void writeStats() {
        final boolean isStatTableUpdateNeeded = ENABLED_FLAG.equals(SysPropertyTool.getSystemProperty(SPARK_OPTION_PREFIX + STAT_TABLE_UPDATE_PROPERTY));

        if (!isStatTableUpdateNeeded) {
            log.info("Stat info about table {} is not needed for Stat Table", datamartTable);
            return;
        }
        final FullTableName resultTable = FullTableName.of(datamartTable.dbName(), STAT_TABLE);

        TableSaveHelper tableSaveHelper = new TableSaveHelper(sqlContext);
        if (!dc.exists(resultTable)) {
            tableSaveHelper.createTable(OD_STATS_SCHEMA, resultTable.fullTableName());
        }

        final Dataset<Row> data = getLoadingInfo();
        tableSaveHelper.insertIntoTable(resultTable.fullTableName(), data, null);
    }

    @VisibleForTesting
    Dataset<Row> getLoadingInfo() {
        return DataFrameUtils.createDF(sqlContext, OD_STATS_SCHEMA,
                row(null,
                        loadingTime,
                        loadingId,
                        null,
                        entityId,
                        (long) StatisticId.CHANGE_STAT_ID.getCode(),
                        null,
                        profile));
    }
}
