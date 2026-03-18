package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.List;
import java.util.Map;

public class ActionConf {
    private String name;
    private String jarName;
    private List<String> extraJarNames;
    private boolean start;
    private String type;
    private String sourceSchema;
    private List<Object> dependencies;
    private String className;
    private String memoryPreset;
    private String driverMemory;
    private String executorMemory;
    private Integer executorCoreNum;
    private Integer executors;
    private Integer driverMemoryOverhead;
    private Integer executorMemoryOverhead;
    private Integer sparkSqlShufflePartitions;
    private Map<String, String> customJavaOpts;

    public ActionConf() {
        //empty constructor because its java bean and used by org.yaml.snakeyaml.Yaml
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJarName() {
        return jarName;
    }

    public void setJarName(String jarName) {
        this.jarName = jarName;
    }

    public List<String> getExtraJarNames() {
        return extraJarNames;
    }

    public void setExtraJarNames(List<String> extraJarNames) {
        this.extraJarNames = extraJarNames;
    }

    public boolean isStart() {
        return start;
    }

    public void setStart(boolean start) {
        this.start = start;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public List<Object> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<Object> dependencies) {
        this.dependencies = dependencies;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMemoryPreset() {
        return memoryPreset;
    }

    public void setMemoryPreset(String preset) {
        this.memoryPreset = preset;
    }

    public String getDriverMemory() {
        return driverMemory;
    }

    public void setDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public Integer getExecutorCoreNum() {
        return executorCoreNum;
    }

    public void setExecutorCoreNum(Integer executorCoreNum) {
        this.executorCoreNum = executorCoreNum;
    }

    public Integer getExecutors() {
        return executors;
    }

    public void setExecutors(Integer executors) {
        this.executors = executors;
    }

    public Integer getDriverMemoryOverhead() {
        return driverMemoryOverhead;
    }

    public void setDriverMemoryOverhead(Integer driverMemoryOverhead) {
        this.driverMemoryOverhead = driverMemoryOverhead;
    }

    public Integer getExecutorMemoryOverhead() {
        return executorMemoryOverhead;
    }

    public void setExecutorMemoryOverhead(Integer executorMemoryOverhead) {
        this.executorMemoryOverhead = executorMemoryOverhead;
    }

    public Integer getSparkSqlShufflePartitions() {
        return sparkSqlShufflePartitions;
    }

    public void setSparkSqlShufflePartitions(Integer sparkSqlShufflePartitions) {
        this.sparkSqlShufflePartitions = sparkSqlShufflePartitions;
    }

    public Map<String, String> getCustomJavaOpts() {
        return customJavaOpts;
    }

    public void setCustomJavaOpts(Map<String, String> customJavaOpts) {
        this.customJavaOpts = customJavaOpts;
    }

    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
