package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumDepFinBillingLocalUfoMover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "depfin-billing-local-ufo-mover-daily";
    }

    @Override
    public String getPath() {
        return "DepFinBillingLocalUfoMover";
    }
}
