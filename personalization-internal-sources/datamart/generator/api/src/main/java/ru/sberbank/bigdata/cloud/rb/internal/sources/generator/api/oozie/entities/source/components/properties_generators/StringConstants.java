package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.components.properties_generators;

public enum StringConstants {
    MAPRED_JOB_QUEUE_NAME("mapred.job.queue.name"),
    YARN_QUEUE_VALUE("${yarnQueue}"),
    PARENT_APP_PATH("parentAppPath"),
    WF_APP_PATH_VALUE("${wf:appPath()}"),
    YARN_MAX_ATTEMPTS("yarn.resourcemanager.am.max-attempts"),
    HCAT_METASTORE_URI("hcat.metastore.uri"),
    HIVE_METASTORE_URI_VALUE("${hiveMetastoreUri}"),
    HCAT_METASTORE_PRINCIPAL("hcat.metastore.principal"),
    PRINCIPAL_VALUE("${principal}"),
    ACTION_FAILED_ERROR_MESSAGE("Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]"),
    CLOUDERA_VERSION("cloudera5.14_compatibility");

    private String value;
    StringConstants(String name){
        value = name;
    }
    public String getValue(){
        return value;
    }

}
