package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEcosysClientAggDay implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-ecosys-client-agg-daily";
    }

    @Override
    public String getPath() {
        return "EcosysClientAggDay";
    }
}
