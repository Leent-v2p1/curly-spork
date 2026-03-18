package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbmgmrkt;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimActionSbmgmrkt implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-action-sbmgmrkt-daily";
    }

    @Override
    public String getPath() {
        return "dim-action-sbmgmrkt";
    }
}
