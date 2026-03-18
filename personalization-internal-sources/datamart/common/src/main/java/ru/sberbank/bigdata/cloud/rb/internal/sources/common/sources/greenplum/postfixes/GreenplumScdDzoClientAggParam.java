package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumScdDzoClientAggParam implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "dzo-client-agg-param-daily";
    }

    @Override
    public String getPath() {
        return "scd-dzo-client-agg-param";
    }
}
