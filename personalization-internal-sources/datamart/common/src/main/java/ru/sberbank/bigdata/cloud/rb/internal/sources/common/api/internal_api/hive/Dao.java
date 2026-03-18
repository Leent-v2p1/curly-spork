package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive;

import java.util.Collection;

public interface Dao {
    void connect();

    boolean executeSql(String sql);

    Collection<String> select(String sql, String colName);

    void disconnect();
}
