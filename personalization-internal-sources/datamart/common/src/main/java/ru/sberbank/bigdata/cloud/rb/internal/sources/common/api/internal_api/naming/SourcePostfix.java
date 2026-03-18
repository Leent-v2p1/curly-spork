package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

public interface SourcePostfix {
    String getPostfix();

    String getPath();

    default String getCtlName() {
        return getPostfix();
    }
}
