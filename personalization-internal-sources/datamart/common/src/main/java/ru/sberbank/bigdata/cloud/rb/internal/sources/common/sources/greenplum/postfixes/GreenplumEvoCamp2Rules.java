package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEvoCamp2Rules implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "evo-camp2-rules-daily";
    }

    @Override
    public String getPath() {
        return "EvoCamp2Rules";
    }
}