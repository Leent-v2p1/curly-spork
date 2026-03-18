package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.ServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

/**
 * Инструкция:
 * 1. Скопировать в CTL какой-нибудь поток.
 * 2. Изменить значение параметра 'oozie.wf.application.path' на '/oozie-app/wf/custom/rb/production/utils/custom-hive-script'
 * 3. Добавить новый параметр 'hive_sql'. Значение параметра д.б. в двойных скобках. Может быть несколько команд, разделённых ';'
 * Пример значения параметра: "drop table test_table; msck repair table some_table;"
 */
public class HiveScriptRunner {

    private static final Logger log = LoggerFactory.getLogger(HiveScriptRunner.class);
    private final SparkSession sqlContext;
    private final String script;

    public HiveScriptRunner(SparkSession sqlContext, String script) {
        this.sqlContext = sqlContext;
        this.script = script;
    }

    public void run() {
        final String[] commands = script.trim().split(";");
        for (String command : commands) {
            command = command.trim();
            log.info("Try to execute: {}", command);
            sqlContext.sql(command);
        }
    }

    public static void main(String[] args) {
        final String commandForExecute = safeSystemProperty("spark.hive.sql");
        LoggerTypeId.set("hive_script_runner");
        final String jobName = "Execute hive script";
        final SparkSession sqlContext = new ServiceFactory(jobName).sqlContext();
        final HiveScriptRunner hiveScriptRunner = new HiveScriptRunner(sqlContext, commandForExecute);
        hiveScriptRunner.run();
    }
}
