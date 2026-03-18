package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.eapteka;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimChannelEapteka implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "dim-channel-eapteka-daily";
    }

    @Override
    public String getPath() {
        return "dim-channel-eapteka";
    }
}
