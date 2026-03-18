package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.mapping;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.EpkSystemCode;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.List;

/**
 * ID клиента ЕПК (Единый профиль клиента) полученный из АС ЕПК.
 */
public class EpkMapping {

    private static final String ACTIVE_FLAG = "1";
    private static final String CENTAUR_FLAG = "1";
    private static final String ACTIVE_DATE = "9999-12-31";

    private final DatamartContext dc;
    private final List<EpkSystemCode> systemCodes;

    public EpkMapping(DatamartContext dc,
                      List<EpkSystemCode> systemCodes) {
        this.dc = dc;
        this.systemCodes = systemCodes;
    }

    public Dataset<Row> build() {
        final Dataset<Row> mapping = dc.sourceTable(FullTableName.of("custom_rb_epk.epk_lnk_host_id"));
        return mapping
                .where(
                        mapping.col("active_flag").equalTo(ACTIVE_FLAG)
                                .and(mapping.col("external_system").isin(codes()))
                                .and(mapping.col("centaur_flag").notEqual(CENTAUR_FLAG))
                                .and(mapping.col("row_actual_to").equalTo(ACTIVE_DATE))
                                .and(mapping.col("epk_id").isNotNull())
                )
                .select(
                        mapping.col("epk_id"),
                        mapping.col("external_system_client_id").as("sys_client_id"),
                        mapping.col("identifier_type")
                );
    }

    private Object[] codes() {
        return systemCodes.stream().map(EpkSystemCode::getCode).toArray();
    }
}
