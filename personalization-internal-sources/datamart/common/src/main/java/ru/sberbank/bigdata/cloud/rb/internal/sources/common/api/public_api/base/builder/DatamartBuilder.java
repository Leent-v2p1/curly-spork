package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.ExtraStatisticAccumulator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.StringReplacer;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFixAPI;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TempSaver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DefaultDatamartFields;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticToFileWriter;

import java.time.LocalDate;

public class DatamartBuilder<T extends Datamart> {

    private static final Logger log = LoggerFactory.getLogger(DatamartBuilder.class);

    private final Class<T> datamartClass;
    private DatamartContext dc;
    private TempSaver tempSaver;
    private StringReplacer emptyStringReplacer;
    private StatisticToFileWriter statisticToFileWriter;
    private ExtraStatisticAccumulator extraStatisticAccumulator;
    private ExtraStatisticAccumulator disabledStatisticAccumulator;
    private DefaultDatamartFields defaultDatamartFields;
    private DataFixAPI dataFixApi;
    private MetastoreService metastoreService;
    //parameters
    private boolean isFirstLoading;
    private boolean isReload;
    private LocalDate buildDateValue;

    public DatamartBuilder(Class<T> datamartClass) {
        this.datamartClass = datamartClass;
    }

    public static <T extends Datamart> DatamartBuilder<T> datamartBuilder(Class<T> datamartClass) {
        return new DatamartBuilder<>(datamartClass);
    }

    public DatamartBuilder<T> setDc(DatamartContext dc) {
        this.dc = dc;
        return this;
    }

    public DatamartBuilder<T> setTempSaver(TempSaver tempSaver) {
        this.tempSaver = tempSaver;
        return this;
    }

    public DatamartBuilder<T> setEmptyStringReplacer(StringReplacer emptyStringReplacer) {
        this.emptyStringReplacer = emptyStringReplacer;
        return this;
    }

    public DatamartBuilder<T> setStatisticToFileWriter(StatisticToFileWriter statisticToFileWriter) {
        this.statisticToFileWriter = statisticToFileWriter;
        return this;
    }

    public DatamartBuilder<T> setExtraStatisticAccumulator(ExtraStatisticAccumulator extraStatisticAccumulator) {
        this.extraStatisticAccumulator = extraStatisticAccumulator;
        return this;
    }

    public DatamartBuilder<T> setDisabledStatisticAccumulator(ExtraStatisticAccumulator disabledStatisticAccumulator) {
        this.disabledStatisticAccumulator = disabledStatisticAccumulator;
        return this;
    }

    public DatamartBuilder<T> setDefaultDatamartFields(DefaultDatamartFields defaultDatamartFields) {
        this.defaultDatamartFields = defaultDatamartFields;
        return this;
    }

    public DatamartBuilder<T> setDataFixApi(DataFixAPI dataFixApi) {
        this.dataFixApi = dataFixApi;
        return this;
    }

    public DatamartBuilder<T> setMetastoreService(MetastoreService metastoreService) {
        this.metastoreService = metastoreService;
        return this;
    }

    public DatamartBuilder<T> setFirstLoading(boolean firstLoading) {
        isFirstLoading = firstLoading;
        log.info("isFirstLoading: {}", isFirstLoading);
        return this;
    }

    public DatamartBuilder<T> setReload(boolean reload) {
        isReload = reload;
        log.info("isReload: {}", isReload);
        return this;
    }

    public DatamartBuilder<T> setBuildDateValue(LocalDate buildDateValue) {
        this.buildDateValue = buildDateValue;
        return this;
    }

    public T create() {
        T datamart;
        try {
            datamart = datamartClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            log.error("Error during instantiating of class " + datamartClass, e);
            throw new IllegalStateException("Error during instantiating of class " + datamartClass, e);
        }

        datamart.setDc(dc);
        datamart.setTempSaver(tempSaver);
        datamart.setEmptyStringReplacer(emptyStringReplacer);
        datamart.setExtraStatisticAccumulator(extraStatisticAccumulator);
        datamart.setDisabledStatisticAccumulator(disabledStatisticAccumulator);
        datamart.setStatisticToFileWriter(statisticToFileWriter);
        datamart.setDefaultDatamartFields(defaultDatamartFields);
        datamart.setDataFixApi(dataFixApi);
        datamart.setMetastoreService(metastoreService);

        datamart.setFirstLoading(isFirstLoading);
        datamart.setReload(isReload);
        datamart.setBuildDateValue(buildDateValue);

        return datamart;
    }
}
