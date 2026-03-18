package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.smartvista.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class SmartvistaArchive implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "archive";
    }

    @Override
    public String getPath() {
        return "archive";
    }
}
