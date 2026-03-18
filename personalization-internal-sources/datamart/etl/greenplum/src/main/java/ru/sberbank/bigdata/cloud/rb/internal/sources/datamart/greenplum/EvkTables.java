package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.TableName;

public enum EvkTables implements TableName {
    DM_UNION_CAMPAIGN_HISTORY,
    REF_UNION_CAMPAIGN_CHANNEL,
    REF_UNION_CAMPAIGN_DIC_HDP;

    @Override
    public String tableName() {
        return super.name().toLowerCase();
    }
}