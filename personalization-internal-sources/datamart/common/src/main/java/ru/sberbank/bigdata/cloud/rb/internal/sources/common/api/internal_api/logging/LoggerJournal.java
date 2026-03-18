package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class LoggerJournal {

    private static final Logger log = LoggerFactory.getLogger(LoggerJournal.class);

    public static final String RESULT_TABLE_PROPERTY = "spark.result.table";
    public static final String RESULT_SCHEMA_PROPERTY = "spark.result.schema";

    public static final String USER_NAME = "spark.user.name";
    public static final String START_TIME = "spark.start.time";
    public static final String YARN_QUEUE = "spark.yarn.queue";
    public static final String CLASS_NAME = "spark.class.name";
    public static final String EXECUTOR_CORES = "spark.executor.cores";
    public static final String EXECUTOR_MEMORY = "spark.executor.memory";
    public static final String DRIVER_MEMORY = "spark.driver.memory";
    public static final String SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";
    public static final String USER_DIR = "user.dir";
    public static final String CTL_LOADING_ID = "spark.ctl.loading.id";
    public static final String HUE_URI = "spark.hueUri";
    public static final String HUE_LINK = "spark.hueLink";

    static final String schemaNameAudit = SysPropertyTool.safeSystemProperty(RESULT_SCHEMA_PROPERTY);
    static final String tableNameAudit = SysPropertyTool.safeSystemProperty(RESULT_TABLE_PROPERTY);

    static final String userName = SysPropertyTool.safeSystemProperty(USER_NAME);
    static final String startTime = SysPropertyTool.safeSystemProperty(START_TIME);
    static final String B_SystemID = "CI2364774";
    static final String SystemID_FP = "CI02320085";

    static final String yarnQueue = SysPropertyTool.safeSystemProperty(YARN_QUEUE);
    static final String className = SysPropertyTool.safeSystemProperty(CLASS_NAME);
    static final String executorCores = SysPropertyTool.safeSystemProperty(EXECUTOR_CORES);
    static final String executorMemory = SysPropertyTool.safeSystemProperty(EXECUTOR_MEMORY);
    static final String driverMemory = SysPropertyTool.safeSystemProperty(DRIVER_MEMORY);
    static final String shufflePartitions = SysPropertyTool.safeSystemProperty(SHUFFLE_PARTITIONS);
    static final String userDir = SysPropertyTool.safeSystemProperty(USER_DIR);
    static final String ctlLoadingId = SysPropertyTool.safeSystemProperty(CTL_LOADING_ID);
    static final String hueUri = SysPropertyTool.safeSystemProperty(HUE_URI);
    static final String hueLink = SysPropertyTool.safeSystemProperty(HUE_LINK);
    static String fqdnAddress;
    static String ipAddress;

    static {
        try {
            fqdnAddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            fqdnAddress = "";
        }
    }

    static {
        try {
            ipAddress = InetAddress.getLocalHost().toString();
        } catch (UnknownHostException e) {
            ipAddress = "";
        }
    }

    private static String[] splittedUserDir = userDir.split("/");
    private static String appId = splittedUserDir[7];
    private static String fullHueLink = hueUri +"/hue/jobbrowser/#!id=" + hueLink;
//start logging
    static final String allParams = "{\"yarnQueue\":" + "\"" + yarnQueue + "\"" +
            ",\"className\":" + "\"" + className + "\"" +
            ",\"userName\":" + "\"" + userName + "\"" +
            ",\"startTime\":" + "\"" + startTime + "\"" +
            ",\"yarnQueue\":" + "\"" + yarnQueue + "\"" +
            ",\"className\":" + "\"" + className + "\"" +
            ",\"executorCores\":" + "\"" + executorCores + "\"" +
            ",\"executorMemory\":" + "\"" + executorMemory + "\"" +
            ",\"driverMemory\":" + "\"" + driverMemory + "\"" +
            ",\"shufflePartitions\":" + "\"" + shufflePartitions + "\"" +
            ",\"appId\":" + "\"" + appId + "\"" +
            ",\"ctlLoadingId\":" + "\"" + ctlLoadingId + "\"" +
            ",\"hueLink\":" + "\"" + fullHueLink + "\"}";

    public static void auditStartApp() {
        log.info("[Audit] [F0] " +
                        "system=\"[\"pprb\",\"soc\"]\" " +
                        "B_SystemID=\"{}\" "+
                        "SystemID_FP=\"{}\" " +
                        "FQDN_ADDRESS=\"{}\" "+
                        "USER_LOGIN=\"{}\" " +
                        "IP_ADDRESS=\"{}\" " +
                        "SESSION_ID=\"{}\" "+
                        "STATUS=\"SUCCESS\" "+
                        "OPERATION_DATE=\"{}\" "+
                        "PARAMS=\"{}\" "+
                        "MESSAGE=\"Старт работы приложения в рамках загрузки {} для расчета витрины {}.{} {}.{} со следующими параметрами {}\"",
                B_SystemID,
                SystemID_FP,
                fqdnAddress,
                userName,
                ipAddress,
                appId,
                startTime,
                allParams,
                ctlLoadingId,
                SystemID_FP,
                B_SystemID,
                schemaNameAudit,
                tableNameAudit,
                allParams);
    }

    public static void auditStgApp(Integer ctlLoadingId, Long rowsCount, Long dirSizeBytes) {
        log.info("[Audit] [C0] " +
                        "system=\"[\"pprb\",\"soc\"]\" " +
                        "B_SystemID=\"{}\" "+
                        "SystemID_FP=\"{}\" " +
                        "FQDN_ADDRESS=\"{}\" "+
                        "USER_LOGIN=\"{}\" " +
                        "IP_ADDRESS=\"{}\" " +
                        "PROCESS_NAME=\"SparkApplication\" " +
                        "SESSION_ID=\"{}\" "+
                        "STATUS=\"SUCCESS\" "+
                        "OPERATION_DATE=\"{}\" "+
                        "OBJECT_NAME=\"{}_stg\" "+
                        "OBJECT_ID=\"{}_reserve\" "+
                        "MESSAGE=\"Изменена таблица {}_stg.{}_reserve в рамках загрузки {}, кол-во записей={}, размер в байтах={}\"",
                B_SystemID,
                SystemID_FP,
                fqdnAddress,
                userName,
                ipAddress,
                appId,
                startTime,
                schemaNameAudit,
                tableNameAudit,
                schemaNameAudit,
                tableNameAudit,
                ctlLoadingId,
                rowsCount,
                dirSizeBytes);
    }

    public static void auditMoveApp(String partitionList) {
        log.info("[Audit] [C0] " +
                        "system=\"[\"pprb\",\"soc\"]\" " +
                        "B_SystemID=\"{}\" "+
                        "SystemID_FP=\"{}\" " +
                        "FQDN_ADDRESS=\"{}\" "+
                        "USER_LOGIN=\"{}\" " +
                        "IP_ADDRESS=\"{}\" " +
                        "PROCESS_NAME=\"SparkApplication\" " +
                        "SESSION_ID=\"{}\" "+
                        "STATUS=\"SUCCESS\" "+
                        "OPERATION_DATE=\"{}\" "+
                        "OBJECT_NAME=\"{}_stg\" "+
                        "OBJECT_ID=\"{}_reserve\" "+
                        "MESSAGE=\"Изменена таблица {}.{} в рамках загрузки {}, перезаписаны партиции=[{}]\"",
                B_SystemID,
                SystemID_FP,
                fqdnAddress,
                userName,
                ipAddress,
                appId,
                startTime,
                schemaNameAudit,
                tableNameAudit,
                schemaNameAudit,
                tableNameAudit,
                ctlLoadingId,
                partitionList);
    }

    public static void auditEndApp() {
        log.info("[Audit] [F0] " +
                        "system=\"[\"pprb\",\"soc\"]\" " +
                        "B_SystemID=\"{}\" "+
                        "SystemID_FP=\"{}\" " +
                        "FQDN_ADDRESS=\"{}\" "+
                        "USER_LOGIN=\"{}\" " +
                        "IP_ADDRESS=\"{}\" " +
                        "SESSION_ID=\"{}\" "+
                        "STATUS=\"SUCCESS\" "+
                        "OPERATION_DATE=\"{}\" "+
                        "PARAMS=\"{}\" "+
                        "MESSAGE=\"Расчет витрины {}.{} {}.{} в рамках загрузки {} завершен со следующими параметрами {}\"",
                B_SystemID,
                SystemID_FP,
                fqdnAddress,
                userName,
                ipAddress,
                appId,
                startTime,
                allParams,
                SystemID_FP,
                B_SystemID,
                schemaNameAudit,
                tableNameAudit,
                ctlLoadingId,
                allParams);
    }

}
