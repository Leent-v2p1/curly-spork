package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.catalog;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.DatamartSaver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlDefaultStatistics;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticPublisherService;

import java.util.Map;
import java.util.Optional;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.getSystemProperty;

/**
 * Common job for uploading catalogs from hdfs to hive table and publish default statistics
 */
public class CatalogUploadJob {

    private static final Logger log = LoggerFactory.getLogger(CatalogUploadJob.class);

    private final DatamartContext datamartContext;
    private final DatamartSaver datamartSaver;
    private final String sourcePath;
    private final StatisticPublisherService statisticsService;
    private final CtlDefaultStatistics ctlDefaultStatistics;
    private final Optional<Integer> ctlEntityId;

    CatalogUploadJob(DatamartContext datamartContext,
                     DatamartSaver datamartSaver,
                     String sourcePath,
                     StatisticPublisherService statisticsService,
                     CtlDefaultStatistics ctlDefaultStatistics,
                     Optional<Integer> ctlEntityId) {
        this.datamartContext = datamartContext;
        this.datamartSaver = datamartSaver;
        this.sourcePath = sourcePath;
        this.statisticsService = statisticsService;
        this.ctlDefaultStatistics = ctlDefaultStatistics;
        this.ctlEntityId = ctlEntityId;
    }

    public Dataset<Row> getDataFrameFromCsv() {
        log.info("Saving catalog to {}", sourcePath);
        return datamartContext
                .context()
                .read()
                .format("com.databricks.spark.csv")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(sourcePath);
    }

    public void run() {
        datamartSaver.save(getDataFrameFromCsv());
        if (ctlEntityId.isPresent()) {
            Map<StatisticId, String> statistics = ctlDefaultStatistics.defaultStatistics();
            log.info("Publishing default statistic for entity {} values are: {}", ctlEntityId, statistics);
            statisticsService.publishStatistics(statistics);
        } else {
            log.info("Not publishing statistic, because entityId is null");
        }
    }

    public static void main(String[] args) {
        final String catalogEnumName = getSystemProperty("spark.catalog.enum.name");
        final Catalog catalogId = Catalog.valueOf(catalogEnumName);
        final DatamartServiceFactory datamartServiceFactory = new DatamartServiceFactory(catalogId);
        LoggerTypeId.set(catalogId);

        final ParametersService parametersService = datamartServiceFactory.parametersService();
        final StatisticPublisherService statisticsService = new StatisticPublisherService(
                datamartServiceFactory.ctlRestApiCalls(),
                parametersService.ctlEntityIdOptional(),
                parametersService.getEnv(),
                parametersService.ctlProfile()
        );

        final DatamartContext context = datamartServiceFactory.datamartContext();
        final DatamartSaver datamartSaver = datamartServiceFactory.datamartSaver();
        final String catalogPath = datamartServiceFactory.pathBuilder().catalogPath(catalogId);
        final Optional<Integer> ctlEntityId = parametersService.ctlEntityIdOptional();
        final CtlDefaultStatistics ctlDefaultStatistics = datamartServiceFactory.ctlDefaultStatistics();
        new CatalogUploadJob(context, datamartSaver, catalogPath, statisticsService, ctlDefaultStatistics, ctlEntityId).run();
    }
}
