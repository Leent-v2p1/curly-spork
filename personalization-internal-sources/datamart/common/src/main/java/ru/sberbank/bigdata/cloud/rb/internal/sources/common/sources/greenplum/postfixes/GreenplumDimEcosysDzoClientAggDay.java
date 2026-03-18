package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumDimEcosysDzoClientAggDay implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-ecosys-dzo-client-agg-day-daily";
    }

    @Override
    public String getPath() {
        return "DimEcosysDzoClientAggDay";
    }
}