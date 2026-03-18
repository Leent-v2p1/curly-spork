package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.appender.epk;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.appender.ClientIdAppender;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_19_0;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.CLIENT_EPK_COLUMN_NAME;

public class EpkIdFromEpkSystem implements ClientIdAppender {

    private final Dataset<Row> epkIdMapping;

    public EpkIdFromEpkSystem(Dataset<Row> epkIdMapping) {
        this.epkIdMapping = epkIdMapping;
    }

    @VisibleForTesting
    public Dataset<Row> getEpkIdMapping() {
        return epkIdMapping;
    }

    @Override
    public Dataset<Row> append(Dataset<Row> toDatamart, String datamartColumnToJoin, String resultMdmFieldName) {
        return toDatamart
                .join(epkIdMapping, toDatamart.col(datamartColumnToJoin)
                        //cast is needed for correct format accordance between join fields
                        .cast(DECIMAL_19_0)
                        .cast(StringType)
                        .equalTo(epkIdMapping.col("sys_client_id")), "left")
                .select(
                        toDatamart.col("*"),
                        coalesce(epkIdMapping.col(CLIENT_EPK_COLUMN_NAME), lit(-1)).as(resultMdmFieldName)
                );
    }
}
