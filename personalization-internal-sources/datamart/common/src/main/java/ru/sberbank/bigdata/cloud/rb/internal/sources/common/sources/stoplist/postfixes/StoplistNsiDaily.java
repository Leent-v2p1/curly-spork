package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.stoplist.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class StoplistNsiDaily implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "nsi-daily";
    }

    @Override
    public String getPath() {
        return "nsi-daily";
    }
}
