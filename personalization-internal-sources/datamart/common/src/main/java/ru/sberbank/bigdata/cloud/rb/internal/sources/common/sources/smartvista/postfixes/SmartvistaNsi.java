package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.smartvista.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class SmartvistaNsi implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "nsi";
    }

    @Override
    public String getPath() {
        return "nsi";
    }
}
