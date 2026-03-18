package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

public class WorkflowUpdate {
    public final Integer id;
    public final String name;
    public final String type;
    public final String engine;
    public final boolean scheduled;
    public final String category;

    public WorkflowUpdate(Integer id, String name, String type, String engine, boolean scheduled, String category) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.engine = engine;
        this.scheduled = scheduled;
        this.category = category;
    }
}
