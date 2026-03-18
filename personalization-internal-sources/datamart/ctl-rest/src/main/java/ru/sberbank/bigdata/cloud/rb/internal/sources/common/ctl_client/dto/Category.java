package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

import java.util.Objects;

public class Category {
    private final int id;
    private final String name;
    private final boolean deleted;
    private final int parentId;
    private final int catId;
    private final String action;

    public Category(int id, String name, boolean deleted, int parentId, int catId, String action) {
        this.id = id;
        this.name = name;
        this.deleted = deleted;
        this.parentId = parentId;
        this.catId = catId;
        this.action = action;
    }

    public int getId() {
        return id;
    }

    public String getName() {
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
        Category category = (Category) o;
        return id == category.id &&
                deleted == category.deleted &&
                parentId == category.parentId &&
                catId == category.catId &&
                Objects.equals(name, category.name) &&
                Objects.equals(action, category.action);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, deleted, parentId, catId, action);
    }
}
