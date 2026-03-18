package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write.FileContent;

import java.util.List;

public class WorkflowWithResources {

    public final List<FileContent> fileContentList;
    public final WORKFLOWAPP workflow;

    public WorkflowWithResources(List<FileContent> shellList, WORKFLOWAPP workflow) {
        this.fileContentList = shellList;
        this.workflow = workflow;
    }

    @Override
    public String toString() {
        return "WorkflowWithResources{" +
                "fileContentList=" + fileContentList +
                ", workflow=" + workflow +
                '}';
    }
}
