package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class FullTableNameImpl implements FullTableName {
    private final String dbName;
    private final String tableName;
    private final String fullTableName;

    public FullTableNameImpl(String dbName, String tableName) {
        this.dbName = requireNonNull(dbName);
        this.tableName = requireNonNull(tableName);
        this.fullTableName = dbName + "." + tableName;
    }

    @Override
    public String dbName() {
        return dbName;
    }

    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public String fullTableName() {
        return fullTableName;
    }

    @Override
    public String toString() {
        return fullTableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FullTableName that = (FullTableName) o;
        return Objects.equals(dbName, that.dbName()) &&
                Objects.equals(tableName, that.tableName()) &&
                Objects.equals(fullTableName, that.fullTableName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, fullTableName);
    }
}
