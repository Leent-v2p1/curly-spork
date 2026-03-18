package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;

import java.time.LocalDate;

public interface IncrementSaveRemover {
    void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean firstLoading);
}
