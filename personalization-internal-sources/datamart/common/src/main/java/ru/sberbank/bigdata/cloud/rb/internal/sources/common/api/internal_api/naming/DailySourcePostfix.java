package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

public class DailySourcePostfix implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "daily";
    }

    @Override
    public String getPath() {
        return "daily";
    }
}
