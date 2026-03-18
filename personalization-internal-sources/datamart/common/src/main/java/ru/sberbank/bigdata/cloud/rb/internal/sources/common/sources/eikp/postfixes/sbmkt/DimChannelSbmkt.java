package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sbmkt;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimChannelSbmkt implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-channel-sbmkt-daily";
    }
    @Override
    public String getPath() {
        return "dim-channel-sbmkt";
    }
}