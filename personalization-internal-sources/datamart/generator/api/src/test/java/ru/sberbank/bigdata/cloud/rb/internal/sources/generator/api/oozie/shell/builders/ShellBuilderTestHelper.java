package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.shell.builders;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;

import java.util.Arrays;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonFiles.PERSONALIZATION_JAR;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.SparkJobParameters.*;

public class ShellBuilderTestHelper {

    public static final String FULL_TABLE = "custom_rb_test.test";
    public static final String TABLE = "test";
    public static final String SCHEMA = "custom_rb_test";
    public static final String MODULE_JAR_NAME = "test-datamart.jar";

    public static ActionConf createConf() {
        ActionConf conf = new ActionConf();
        conf.setName("spark-app-name");
        conf.setJarName(MODULE_JAR_NAME);
        conf.setExtraJarNames(Arrays.asList(PERSONALIZATION_JAR, MODULE_JAR_NAME));
        conf.setType(WorkflowType.DATAMART.getKey());
        conf.setSourceSchema("custom_rb_test");
        conf.setClassName("ru.my.Class");
        conf.setMemoryPreset("MID");
        conf.setDriverMemory(MIN_DRIVER_MEMORY);
        conf.setExecutorMemory(MID_EXECUTOR_MEMORY);
        conf.setExecutors(NUMBER_OF_EXECUTORS);
        conf.setDriverMemoryOverhead(MIN_DRIVER_OVERHEAD);
        conf.setExecutorMemoryOverhead(MIN_EXECUTOR_OVERHEAD);
        conf.setSparkSqlShufflePartitions(SHUFFLE_PARTITIONS);
        return conf;
    }

    public static <T extends ActionConf> T createConf(Class<T> confClass) {
        T conf = null;
        try {
            conf = confClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        conf.setName("spark-app-name");
        conf.setType(WorkflowType.DATAMART.getKey());
        conf.setSourceSchema("custom_rb_test");
        conf.setClassName("ru.my.Class");
        conf.setDriverMemory(MIN_DRIVER_MEMORY);
        conf.setExecutorMemory(MID_EXECUTOR_MEMORY);
        conf.setExecutorCoreNum(MID_EXECUTOR_CORE_NUM);
        conf.setExecutors(MID_NUMBER_OF_EXECUTORS);
        conf.setDriverMemoryOverhead(MIN_DRIVER_OVERHEAD);
        conf.setExecutorMemoryOverhead(MIN_EXECUTOR_OVERHEAD);
        conf.setSparkSqlShufflePartitions(SHUFFLE_PARTITIONS);
        return conf;
    }
}
