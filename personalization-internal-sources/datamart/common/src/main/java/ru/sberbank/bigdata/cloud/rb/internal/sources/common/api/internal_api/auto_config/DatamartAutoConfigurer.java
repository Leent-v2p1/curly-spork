package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.HistoryUpdate;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.Increment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.builder.DatamartBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.*;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.*;

public class DatamartAutoConfigurer {
    private static final Logger log = LoggerFactory.getLogger(DatamartAutoConfigurer.class);

    private final DatamartServiceFactory dsf;

    public DatamartAutoConfigurer(DatamartServiceFactory dsf) {
        this.dsf = dsf;
    }

    public <T extends Datamart> T createDatamart(Class<T> datamartClass) {
        log.info("Start configuration of {}", datamartClass);
        final T datamart = DatamartBuilder.datamartBuilder(datamartClass)
                .setDc(dsf.datamartContext())
                .setFirstLoading(dsf.parametersService().isFirstLoading())
                .setReload(dsf.parametersService().forceReload())
                .setTempSaver(dsf.tempSaver())
                .setEmptyStringReplacer(dsf.emptyStringReplacer())
                .setExtraStatisticAccumulator(dsf.extraStatisticAccumulator())
                .setDisabledStatisticAccumulator(dsf.disabledStatisticAccumulator())
                .setStatisticToFileWriter(dsf.statisticToFileWriter())
                .setDefaultDatamartFields(dsf.defaultDatamartParameters())
                .setBuildDateValue(dsf.parametersService().ctlBuildDate())
                .setDataFixApi(dsf.dataFixApi())
                .setMetastoreService(dsf.metastoreService())
                .create();

        final HiveSavingStrategy defaultSavingStrategy = getDefaultStrategy(datamart);
        final HiveSavingStrategy customSavingStrategy = datamart.customSavingStrategy(dsf);
        final HiveSavingStrategy resultSavingStrategy = savingStrategy(datamartClass, defaultSavingStrategy, customSavingStrategy);
        datamart.setDatamartSaver(dsf.datamartSaver(resultSavingStrategy));

        datamart.init(dsf);
        return datamart;
    }

    private <T extends Datamart> HiveSavingStrategy savingStrategy(Class<T> datamartClass,
                                                                   HiveSavingStrategy defaultSavingStrategy,
                                                                   HiveSavingStrategy customSavingStrategy) {
        final HiveSavingStrategy savingStrategy = customSavingStrategy != null ? customSavingStrategy : defaultSavingStrategy;
        final Class<? extends Datamart> superclass = (Class<? extends Datamart>) datamartClass.getSuperclass();
        final boolean isSavingStrategySupported = StrategyValidator.isSavingStrategySupported(superclass, savingStrategy.getClass());
        if (!isSavingStrategySupported) {
            throw new IllegalStateException("Superclass of dm " + datamartClass.getName()
                    + ": " + superclass + " not supported by selected saving strategy " + savingStrategy.getClass());
        }
        return savingStrategy;
    }

    private HiveSavingStrategy getDefaultStrategy(Datamart datamart) {
        ParametersService parametersService = dsf.parametersService();
        final Class<?> datamartClass = datamart.getClass();
        final Class superclass = datamartClass.getSuperclass();
        final boolean historyStrategy = datamartClass.isAnnotationPresent(HistoryUpdate.class);
        if (historyStrategy && superclass.equals(ReplicaBasedHistoricalDatamart.class)) {
            log.info("Modifying ReplicaBasedHistoricalDatamart.class");
            ReplicaBasedHistoricalDatamart castedDm = (ReplicaBasedHistoricalDatamart) datamart;
            castedDm.setRecoveryMode(parametersService.recoveryMode());
            castedDm.setReplicaNameResolver(dsf.replicaNameResolver());
            castedDm.setKeyChecker(dsf.keyChecker());
            castedDm.setReplicaActualityDate(parametersService.replicaActualityDate());
            castedDm.setLoadingId(parametersService.ctlLoadingId());
            return new SeparateHistoryTableSavingStrategy();
        }
        if (historyStrategy && superclass.equals(HistoricalDatamart.class)) {
            log.info("Modifying HistoricalDatamart.class");
            HistoricalDatamart castedDm = (HistoricalDatamart) datamart;
            castedDm.setKeyChecker(dsf.keyChecker());
            castedDm.setReplicaActualityDate(parametersService.replicaActualityDate());
            castedDm.setLoadingId(parametersService.ctlLoadingId());
            return new SeparateHistoryTableSavingStrategy();
        }
        if (superclass.equals(ReplicaBasedStagingDatamart.class)) {
            log.info("Modifying ReplicaBasedStagingDatamart.class");

            ReplicaBasedStagingDatamart castedDm = (ReplicaBasedStagingDatamart) datamart;
            castedDm.setRecoveryMode(parametersService.recoveryMode());
            castedDm.setReplicaNameResolver(dsf.replicaNameResolver());
            return new OverwriteSavingStartegy();
        }
        if (superclass.equals(StagingDatamart.class)) {
            log.info("Modifying StagingDatamart.class");

            StagingDatamart castedDm = (StagingDatamart) datamart;
            castedDm.setCtlDefaultStatistics(dsf.ctlDefaultStatistics());
            castedDm.setStatisticMerger(dsf.statisticMerger());
            castedDm.setStatisticPublisherService(dsf.statisticPublisherService());
            return new OverwriteSavingStartegy();
        }

        boolean reservingStrategy = datamartClass.isAnnotationPresent(Increment.class)
                || datamartClass.isAnnotationPresent(FullReplace.class);
        if (reservingStrategy && superclass.equals(Datamart.class)) {
            log.info("Modifying Datamart.class");

            return new ReservingSavingStrategy();
        }
        final PartialReplace partialReplace = datamartClass.getAnnotation(PartialReplace.class);
        if (partialReplace != null && superclass.equals(Datamart.class)) {
            log.info("Modifying Datamart.class");
            if (partialReplace.partitioning().equals(MONTH_PART)) {
                return PartitionedSavingStrategy.withMonthPart();
            }
            if (partialReplace.partitioning().equals(DAY_PART)) {
                return PartitionedSavingStrategy.withDayPart();
            }
            if (partialReplace.partitioning().equals(CTL_LOADING_PART)) {
                return PartitionedSavingStrategy.withCtlLoadingPartPart();
            }
            if (partialReplace.partitioning().equals(ERIB_SCHEMA)) {
                return PartitionedSavingStrategy.withEribSchemaPart();
            }
            return new ReservingSavingStrategy();
        }
        throw new IllegalStateException("Superclass " + superclass + " of datamart class " + datamart.getClass() + " is not supported.");
    }
}
