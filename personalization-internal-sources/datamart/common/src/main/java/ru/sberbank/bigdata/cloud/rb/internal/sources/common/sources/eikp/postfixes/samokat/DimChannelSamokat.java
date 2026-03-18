package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.samokat;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimChannelSamokat implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "dim-channel-samokat-daily";
    }

    @Override
    public String getPath() {
        return "dim-channel-samokat";
    }
}
