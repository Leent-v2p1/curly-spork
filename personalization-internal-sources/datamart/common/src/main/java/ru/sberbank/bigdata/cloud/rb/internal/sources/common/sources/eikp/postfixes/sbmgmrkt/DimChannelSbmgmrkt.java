package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbmgmrkt;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimChannelSbmgmrkt implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-channel-sbmgmrkt-daily";
    }

    @Override
    public String getPath() {
        return "dim-channel-sbmgmrkt";
    }
}
