package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.internal;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.BuildRequiredChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.ToDeleteDir;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlDefaultStatistics;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticToFileWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.StatTableWriter;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Builder, создающий экземпляр ReserveToPaMover или HistoricalReserveToPaMover в зависимости от типа витрины.
 */
public class ReservingBuilder {

    protected static final Logger log = LoggerFactory.getLogger(ReservingBuilder.class);

    private DatamartServiceFactory serviceFactory;
    private SparkSession context;
    private DatamartNaming naming;
    private ToDeleteDir dirToDelete;
    private boolean recoveryMode;
    private Optional<LocalDate> recoveryDate;
    private LocalDate buildDate;
    private boolean firstLoading;
    private IncrementSaveRemover incrementSaveRemover;
    private boolean testPartiallyHistoryUpdate;
    private BuildRequiredChecker checker;
    private boolean isRepartitionNeeded;
    private StatisticToFileWriter statisticToFileWriter;
    private CtlDefaultStatistics ctlDefaultStatistics;
    private StatTableWriter statTableWriter;

    ReservingBuilder(DatamartServiceFactory serviceFactory) {
        this.serviceFactory = serviceFactory;
    }

    private static DatamartNaming getDatamartNaming(DatamartNaming naming, Environment environment) {
        if (naming.fullTableName().equals("custom_rb_smartvista.card_transaction_archive")) {
            log.info("Custom naming for card_transaction_archive: change resultTable to card_transaction.");
            String sourceSchema = naming.sourceSchema();
            String targetSchema = naming.resultSchema();
            String targetTable = "card_transaction";
            naming = new DatamartNaming(sourceSchema, targetSchema, targetTable, environment) {
                @Override
                public String reserveFullTableName() {
                    String cardTransactionArchiveReserve = stage("card_transaction_archive_reserve");
                    logValue("reserveFullTableName", cardTransactionArchiveReserve);
                    return cardTransactionArchiveReserve;
                }
            };
        }
        return naming;
    }

    public ReservingBuilder context(SparkSession context) {
        this.context = context;
        return this;
    }

    public ReservingBuilder naming(DatamartNaming naming, Environment environment) {
        this.naming = getDatamartNaming(naming, environment);
        return this;
    }

    public ReservingBuilder toDeleteDir(ToDeleteDir dirToDelete) {
        this.dirToDelete = dirToDelete;
        return this;
    }

    public ReservingBuilder recoveryMode(boolean recoveryMode) {
        this.recoveryMode = recoveryMode;
        return this;
    }

    public ReservingBuilder recoveryDate(Optional<LocalDate> recoveryDate) {
        this.recoveryDate = recoveryDate;
        return this;
    }

    public ReservingBuilder isFirstLoading(boolean firstLoading) {
        this.firstLoading = firstLoading;
        return this;
    }

    public ReservingBuilder incrementSaveRemover(IncrementSaveRemover incrementSaveRemover) {
        this.incrementSaveRemover = incrementSaveRemover;
        return this;
    }

    public ReservingBuilder testPartiallyHistoryUpdate(boolean testPartiallyHistoryUpdate) {
        this.testPartiallyHistoryUpdate = testPartiallyHistoryUpdate;
        return this;
    }

    public ReservingBuilder buildRequiredChecker(BuildRequiredChecker checker) {
        this.checker = checker;
        return this;
    }

    public ReservingBuilder isRepartitionNeeded(boolean isRepartitionNeeded) {
        this.isRepartitionNeeded = isRepartitionNeeded;
        return this;
    }

    public ReservingBuilder statisticToFileWriter(StatisticToFileWriter statisticToFileWriter) {
        this.statisticToFileWriter = statisticToFileWriter;
        return this;
    }

    public ReservingBuilder ctlDefaultStatistics(CtlDefaultStatistics ctlDefaultStatistics) {
        this.ctlDefaultStatistics = ctlDefaultStatistics;
        return this;
    }

    public ReservingBuilder buildDate(LocalDate buildDate) {
        this.buildDate = buildDate;
        return this;
    }

    public ReservingBuilder statTableWriter(StatTableWriter statTableWriter) {
        this.statTableWriter = statTableWriter;
        return this;
    }

    public ReserveToPaMover create() {
        ReservingParameters reservingParameters = new ReservingParameters(
                dirToDelete,
                recoveryDate,
                recoveryMode,
                firstLoading,
                testPartiallyHistoryUpdate,
                isRepartitionNeeded,
                buildDate);
        if (serviceFactory.parametersService().isHistorical()) {
            return new HistoricalReserveToPaMover(
                    context,
                    naming,
                    incrementSaveRemover,
                    checker,
                    statisticToFileWriter,
                    ctlDefaultStatistics,
                    statTableWriter,
                    reservingParameters
            );
        } else {
            return new ReserveToPaMover(
                    context,
                    naming,
                    incrementSaveRemover,
                    checker,
                    statisticToFileWriter,
                    ctlDefaultStatistics,
                    statTableWriter,
                    reservingParameters
            );
        }
    }
}
