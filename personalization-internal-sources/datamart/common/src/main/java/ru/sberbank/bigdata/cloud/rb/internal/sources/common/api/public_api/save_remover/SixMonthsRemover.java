package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;

import java.time.LocalDate;

public class SixMonthsRemover extends CustomOlderMonthPartitionRemover {
    @Override
    public void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean firstLoading) {
        remove("start_dt", context, naming, buildDate, firstLoading, 6);
    }
}
