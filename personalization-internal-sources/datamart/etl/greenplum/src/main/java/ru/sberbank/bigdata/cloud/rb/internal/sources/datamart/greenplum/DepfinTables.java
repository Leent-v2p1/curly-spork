package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.TableName;

public enum DepfinTables implements TableName {
    FT_ECOSYS_TXN_DET,
    DIM_ECOSYS_DZO_CLIENT_AGG_DAY;

    @Override
    public String tableName() {
        return super.name().toLowerCase();
    }
}