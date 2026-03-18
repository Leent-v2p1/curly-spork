package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.smartvista.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class SmartvistaTpsDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "tps-daily";
    }

    @Override
    public String getPath() {
        return "tps-daily";
    }
}
