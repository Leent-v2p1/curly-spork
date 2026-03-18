package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

public class MonthlySourcePostfix implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "monthly";
    }

    @Override
    public String getPath() {
        return "monthly";
    }
}
