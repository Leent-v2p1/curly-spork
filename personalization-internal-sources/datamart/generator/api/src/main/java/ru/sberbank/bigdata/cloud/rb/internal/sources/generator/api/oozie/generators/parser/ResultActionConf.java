package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser;

public class ResultActionConf extends ActionConf {

    private String workflowName;

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    @Override
    public String toString() {
        return "ResultActionConf{" +
                "workflowName='" + workflowName + '\'' +
                '}';
    }
}
