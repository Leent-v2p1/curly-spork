package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming;

public class TableNameImpl implements TableName {
    private final String name;

    public TableNameImpl(String name) {
        this.name = name;
    }

    @Override
    public String tableName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableNameImpl tableName = (TableNameImpl) o;

        return name != null ? name.equals(tableName.name) : tableName.name == null;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
