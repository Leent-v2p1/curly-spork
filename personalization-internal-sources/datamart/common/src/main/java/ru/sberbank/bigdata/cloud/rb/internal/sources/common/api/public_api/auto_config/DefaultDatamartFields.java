package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.Timestamp;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.lit;

/**
 * Appends defaults fields to datamart, this fields are: ctl_loading, ctl_validFrom, ctl_action
 */
public class DefaultDatamartFields {

    public static final String INSERT_ACTION_FLAG = "I";

    private final int loadingId;
    private final LocalDate validFromDate;

    public DefaultDatamartFields(int loadingId, LocalDate validFromDate) {
        this.loadingId = loadingId;
        this.validFromDate = validFromDate;
    }

    public Dataset<Row> addDefault(Dataset<Row> datamart) {
        return datamart.select(
                datamart.col("*"),
                lit(loadingId).as("ctl_loading"),
                lit(Timestamp.valueOf(validFromDate.atStartOfDay())).as("ctl_validfrom"),
                lit(INSERT_ACTION_FLAG).as("ctl_action")
        );
    }
}
