package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.ekp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EpkAdditionalEpkDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "additional-epk-daily";
    }

    @Override
    public String getPath() {
        return "additional-epk-daily";
    }
}
