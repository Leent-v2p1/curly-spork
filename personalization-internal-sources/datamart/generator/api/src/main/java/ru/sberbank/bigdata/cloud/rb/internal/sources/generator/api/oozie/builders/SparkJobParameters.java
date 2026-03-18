package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders;

public class SparkJobParameters {
    public static final String MIN_EXECUTOR_MEMORY = "2G";
    public static final Integer MIN_NUMBER_OF_EXECUTORS = 1;
    public static final String MIN_DRIVER_MEMORY = "2G";
    public static final Integer MIN_SHUFFLE_PARTITIONS = 200;
    public static final Integer MIN_EXECUTOR_OVERHEAD = 400;
    public static final Integer MIN_DRIVER_OVERHEAD = 400;

    public static final String EXECUTOR_MEMORY = "2G";
    public static final Integer NUMBER_OF_EXECUTORS = 50;
    public static final Integer EXECUTOR_CORE_NUM = 1;
    public static final String DRIVER_MEMORY = "4G";
    public static final Integer SHUFFLE_PARTITIONS = 400;
    public static final Integer EXECUTOR_OVERHEAD = 900;
    public static final Integer DRIVER_OVERHEAD = 1600;

    //presets
    public static final String LOW_EXECUTOR_MEMORY = "2G";
    public static final String MID_EXECUTOR_MEMORY = "4G";
    public static final String HIGH_EXECUTOR_MEMORY = "8G";

    public static final Integer LOW_EXECUTOR_CORE_NUM = 1;
    public static final Integer MID_EXECUTOR_CORE_NUM = 1;
    public static final Integer HIGH_EXECUTOR_CORE_NUM = 1;

    public static final Integer LOWEST_NUMBER_OF_EXECUTORS = 8;
    public static final Integer LOW_NUMBER_OF_EXECUTORS = 20;
    public static final Integer MID_NUMBER_OF_EXECUTORS = 40;
    public static final Integer HIGH_NUMBER_OF_EXECUTORS = 80;

    public static final Integer MAX_ATTEMPTS = 1;

    public static final int SHUFFLE_PARTITIONS_FOR_TEST_BUILD = 2;
    public static final int NUMBER_OF_EXECUTORS_FOR_TEST_BUILD = 2;
}
