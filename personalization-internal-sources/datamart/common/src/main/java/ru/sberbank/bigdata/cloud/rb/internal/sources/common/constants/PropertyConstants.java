package ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants;

import org.apache.derby.iapi.sql.dictionary.StatementRolePermission;

public class PropertyConstants {

    public static final String ENABLED_FLAG = "enabled";
    public static final String REPARTITION_PROPERTY = "repartition";
    public static final String DQCHECK_PROPERTY = "dqcheck";
    public static final String KAFKA_PROPERTY = "kafka";
    public static final String CTL_ENTITY_ID_PROPERTY = "ctl.entity.id";

    public static final String CATALOG_ENUM_NAME = "spark.catalog.enum.name";
    public static final String SPARK_OPTION_PREFIX = "spark.";

    public static final String PROPERTY_SEPARATOR = "=";
    public static final String ALLOW_FOR_ALL_PREFIX = "datamart.all.";
    public static final String SPARK_CONF_PATH = "/etc/spark3/conf/spark-defaults.conf";
    public static final String SAVE_SALTING_PROPERTY = "save_salting";
    public static final String STAT_TABLE_UPDATE_PROPERTY = "stat_table_update";

    public static final String DEFAULT_VALUE_CTL_VERSION_V5 = "false";
}
