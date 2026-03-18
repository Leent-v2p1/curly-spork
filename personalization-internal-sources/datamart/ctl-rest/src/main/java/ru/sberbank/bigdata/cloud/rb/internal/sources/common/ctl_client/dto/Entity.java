package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

public class Entity {
    public final int id;
    public final String name;
    public final String path;
    public final String storage;
    public final int parentId;

    public Entity(int id, String name, String path, String storage, int parentId) {
        this.id = id;
        this.name = name;
        this.path = path;
        this.storage = storage;
        this.parentId = parentId;
    }
}
