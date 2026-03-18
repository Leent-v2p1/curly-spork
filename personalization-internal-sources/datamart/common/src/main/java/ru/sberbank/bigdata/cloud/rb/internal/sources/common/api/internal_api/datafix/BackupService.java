package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.ToDeleteDir;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.copyTable;

/**
 * Класс создаёт бекап таблицы, перед применением дата-фиксов.
 * Если витрина историческая, то создаётся бекап hist, а бекап snp нет
 */
public class BackupService {

    private static final Logger log = LoggerFactory.getLogger(BackupService.class);

    private final SparkSession context;
    private final DatamartNaming naming;
    private final ToDeleteDir toDeleteDir;
    private final boolean isDatamartHistorical;

    public BackupService(SparkSession context,
                         DatamartNaming naming,
                         ToDeleteDir toDeleteDir,
                         boolean isDatamartHistorical) {
        this.context = context;
        this.naming = naming;
        this.toDeleteDir = toDeleteDir;
        this.isDatamartHistorical = isDatamartHistorical;
    }

    void createBackup() {
        copyTable(context, naming.fullTableName(), naming.backupFullTableName());
        if (isDatamartHistorical) {
            copyTable(context, naming.historyFullTableName(), naming.historyBackupFullTableName());
        }
    }

    void restoreTableFromBackup() {
        copyTable(context, naming.backupFullTableName(), naming.fullTableName());
        if (isDatamartHistorical) {
            copyTable(context, naming.historyBackupFullTableName(), naming.historyFullTableName());
        }
    }
}
