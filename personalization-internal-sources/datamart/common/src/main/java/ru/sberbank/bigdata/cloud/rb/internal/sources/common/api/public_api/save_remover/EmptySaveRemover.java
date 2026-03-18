package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;

import java.time.LocalDate;

public class EmptySaveRemover implements IncrementSaveRemover {
    @Override
    public void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean isFirstLoading) {
        //do nothing
    }
}
