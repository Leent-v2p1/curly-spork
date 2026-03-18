package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.SparkJobParameters.*;

public class MemoryParams {

    private final String driverMemory;
    private final String executorMemory;
    private final Integer executorCoreNum;
    private final Integer numberOfExecutors;
    private final Integer driverMemoryOverhead;
    private final Integer executorMemoryOverhead;
    private final Integer sparkShufflePartitions;

    private MemoryParams(String driverMemory,
                         String executorMemory,
                         Integer executorCoreNum,
                         Integer numberOfExecutors,
                         Integer driverMemoryOverhead,
                         Integer executorMemoryOverhead,
                         Integer sparkShufflePartitions) {
        this.driverMemory = driverMemory;
        this.executorMemory = executorMemory;
        this.executorCoreNum = executorCoreNum;
        this.numberOfExecutors = numberOfExecutors;
        this.driverMemoryOverhead = driverMemoryOverhead;
        this.executorMemoryOverhead = executorMemoryOverhead;
        this.sparkShufflePartitions = sparkShufflePartitions;
    }

    public static MemoryParams minimalMemoryParams() {
        return new MemoryParams(MIN_DRIVER_MEMORY, MIN_EXECUTOR_MEMORY, LOW_EXECUTOR_CORE_NUM, NUMBER_OF_EXECUTORS_FOR_TEST_BUILD, MIN_DRIVER_OVERHEAD, MIN_EXECUTOR_OVERHEAD, SHUFFLE_PARTITIONS_FOR_TEST_BUILD);
    }

    public static MemoryParams defaultMemoryParams() {
        return new MemoryParams(DRIVER_MEMORY, EXECUTOR_MEMORY, EXECUTOR_CORE_NUM, NUMBER_OF_EXECUTORS, DRIVER_OVERHEAD, EXECUTOR_OVERHEAD, SHUFFLE_PARTITIONS);
    }

    public static MemoryParams lowestPreset() {
        return new MemoryParams(DRIVER_MEMORY, LOW_EXECUTOR_MEMORY, EXECUTOR_CORE_NUM, LOWEST_NUMBER_OF_EXECUTORS, MIN_DRIVER_OVERHEAD, MIN_EXECUTOR_OVERHEAD, MIN_SHUFFLE_PARTITIONS);
    }

    public static MemoryParams lowPreset() {
        return new MemoryParams(DRIVER_MEMORY, LOW_EXECUTOR_MEMORY, EXECUTOR_CORE_NUM, LOW_NUMBER_OF_EXECUTORS, MIN_DRIVER_OVERHEAD, MIN_EXECUTOR_OVERHEAD, MIN_SHUFFLE_PARTITIONS);
    }

    public static MemoryParams midPreset() {
        return new MemoryParams(DRIVER_MEMORY, MID_EXECUTOR_MEMORY, MID_EXECUTOR_CORE_NUM, MID_NUMBER_OF_EXECUTORS, DRIVER_OVERHEAD, EXECUTOR_OVERHEAD, SHUFFLE_PARTITIONS);
    }

    public static MemoryParams highPreset() {
        return new MemoryParams(DRIVER_MEMORY, HIGH_EXECUTOR_MEMORY, MID_EXECUTOR_CORE_NUM, HIGH_NUMBER_OF_EXECUTORS, DRIVER_OVERHEAD, EXECUTOR_OVERHEAD * 2, SHUFFLE_PARTITIONS);
    }

    public static MemoryParams createAccordingToPropertiesService(DatamartProperties properties) {
        return memoryParamsBasedOnPropertiesService(properties, defaultMemoryParams());
    }

    public static MemoryParams memoryParamsBasedOnPropertiesService(DatamartProperties properties, MemoryParams defaultPreset) {
        MemoryParams preset = resolvePreset(properties.getMemoryPreset(), defaultPreset);
        return new MemoryParams(
                Optional.ofNullable(properties.getDriverMemory()).orElse(preset.driverMemory),
                Optional.ofNullable(properties.getExecutorMemory()).orElse(preset.executorMemory),
                Optional.ofNullable(properties.getExecutorCoreNum()).orElse(preset.executorCoreNum),
                Optional.ofNullable(properties.getExecutors()).orElse(preset.numberOfExecutors),
                Optional.ofNullable(properties.getDriverMemoryOverhead()).orElse(preset.driverMemoryOverhead),
                Optional.ofNullable(properties.getExecutorMemoryOverhead()).orElse(preset.executorMemoryOverhead),
                Optional.ofNullable(properties.getSparkSqlShufflePartitions()).orElse(preset.sparkShufflePartitions)
        );
    }

    public static MemoryParams memoryParamsBasedOnActionConf(ActionConf actionConf, MemoryParams defaultPreset) {
        MemoryParams preset = resolvePreset(actionConf.getMemoryPreset(), defaultPreset);
        return new MemoryParams(
                Optional.ofNullable(actionConf.getDriverMemory()).orElse(preset.driverMemory),
                Optional.ofNullable(actionConf.getExecutorMemory()).orElse(preset.executorMemory),
                Optional.ofNullable(actionConf.getExecutorCoreNum()).orElse(preset.executorCoreNum),
                Optional.ofNullable(actionConf.getExecutors()).orElse(preset.numberOfExecutors),
                Optional.ofNullable(actionConf.getDriverMemoryOverhead()).orElse(preset.driverMemoryOverhead),
                Optional.ofNullable(actionConf.getExecutorMemoryOverhead()).orElse(preset.executorMemoryOverhead),
                Optional.ofNullable(actionConf.getSparkSqlShufflePartitions()).orElse(preset.sparkShufflePartitions)
        );
    }

    public static MemoryParams memoryParamsForRepartitioner(ActionConf actionConf, MemoryParams defaultPreset) {
        MemoryParams preset = resolvePreset(actionConf.getMemoryPreset(), defaultPreset);
        return new MemoryParams(
                Optional.ofNullable(actionConf.getDriverMemory()).orElse(preset.driverMemory),
                Optional.ofNullable(actionConf.getExecutorMemory()).orElse(preset.executorMemory),
                LOW_EXECUTOR_CORE_NUM,
                Optional.ofNullable(actionConf.getExecutors()).orElse(preset.numberOfExecutors),
                Optional.ofNullable(actionConf.getDriverMemoryOverhead()).orElse(preset.driverMemoryOverhead),
                Optional.ofNullable(actionConf.getExecutorMemoryOverhead()).orElse(preset.executorMemoryOverhead),
                Optional.ofNullable(actionConf.getSparkSqlShufflePartitions()).orElse(preset.sparkShufflePartitions)
        );
    }

    private static MemoryParams resolvePreset(String actionPresetConf, MemoryParams defaultPreset) {
        if (actionPresetConf != null) {
            return MemoryPreset.valueOf(actionPresetConf).getParams();
        }
        return defaultPreset;
    }

    public String getDriverMemory() {
        return driverMemory;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public Integer getExecutorCoreNum() {
        return executorCoreNum;
    }

    public Integer getNumberOfExecutors() {
        return numberOfExecutors;
    }

    public Integer getDriverMemoryOverhead() {
        return driverMemoryOverhead;
    }

    public Integer getExecutorMemoryOverhead() {
        return executorMemoryOverhead;
    }

    public Integer getSparkShufflePartitions() {
        return sparkShufflePartitions;
    }

    @Override
    public String toString() {
        return "MemoryParams{" +
                "driverMemory='" + driverMemory + '\'' +
                ", executorMemory='" + executorMemory + '\'' +
                ", numberOfExecutors=" + numberOfExecutors +
                ", driverMemoryOverhead=" + driverMemoryOverhead +
                ", executorMemoryOverhead=" + executorMemoryOverhead +
                ", sparkShufflePartitions=" + sparkShufflePartitions +
                '}';
    }

    public Map<String, Object> toMap() {
        HashMap<String, Object> result = new HashMap<>();
        result.put("executorMemory", getExecutorMemory());
        result.put("executorCoreNum", getExecutorCoreNum());
        result.put("numExecutors", getNumberOfExecutors());
        result.put("driverMemory", getDriverMemory());
        result.put("attempts", MAX_ATTEMPTS);
        result.put("executorMemoryOverhead", getExecutorMemoryOverhead());
        result.put("shufflePartitions", getSparkShufflePartitions());
        result.put("driverMemoryOverhead", getDriverMemoryOverhead());
        return result;
    }
}
