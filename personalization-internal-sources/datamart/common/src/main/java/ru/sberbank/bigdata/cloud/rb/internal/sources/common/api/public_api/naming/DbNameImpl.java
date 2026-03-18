package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming;

import java.util.Objects;

public class DbNameImpl implements DbName {
    private final String dbName;

    public DbNameImpl(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public String dbName() {
        return dbName;
    }

    public FullTableName resolve(String tableName) {
        return new FullTableNameImpl(dbName, tableName);
    }

    public FullTableName resolve(TableName tableName) {
        return resolve(tableName.tableName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DbNameImpl)) {
            return false;
        }
        DbNameImpl dbName1 = (DbNameImpl) o;
        return Objects.equals(dbName, dbName1.dbName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName);
    }
}
