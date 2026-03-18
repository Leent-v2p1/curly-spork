package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.Checker;

public interface FullTableName extends DbName, TableName {
    static FullTableName of(String fullTableName) {
        final String[] names = fullTableName.split("\\.");
        Checker.checkCondition((names.length != 2), "name should contains 2 parts");
        return new FullTableNameImpl(names[0], names[1]);
    }

    static FullTableName of(String dbName, String tableName) {
        return new FullTableNameImpl(dbName, tableName);
    }

    String fullTableName();
}
