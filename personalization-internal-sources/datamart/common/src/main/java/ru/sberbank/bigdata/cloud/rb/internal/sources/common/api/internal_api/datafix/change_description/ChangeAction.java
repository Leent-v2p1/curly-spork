package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.change_description;

import org.apache.spark.sql.types.StructField;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFix;

public interface ChangeAction extends DataFix {

    StructField from();

    StructField to();
}
