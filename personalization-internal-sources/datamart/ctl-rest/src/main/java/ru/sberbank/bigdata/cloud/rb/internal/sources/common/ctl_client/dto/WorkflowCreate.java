package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto;

public class WorkflowCreate {
    public final String name;
    public final String type;
    public final String engine;
    public final boolean scheduled;
    public final String category;
    public final String profile;

    public WorkflowCreate(String name, String type, String engine, boolean scheduled, String category, String profile) {
        this.name = name;
        this.type = type;
        this.engine = engine;
        this.scheduled = scheduled;
        this.category = category;
        this.profile = profile;
    }
}
