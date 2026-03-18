package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;

import java.util.*;
import java.util.stream.Collectors;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_SCHEMA_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_TABLE_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.STATISTIC_PUBLISHER_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

/**
 * Service takes content of file <code>pathToStatisticFile</code>, removes statistics that contains in
 * file <code>pathToDisabledStatisticFile</code>, verifies that statistics written in required format,
 * publishes this statistics to ctl and delete all files that start with 'statistic_'
 * <p>
 * expected file format:
 * code=value with spaces
 * example:
 * 15=2020-01-20 16:22:05
 */
public class CtlStatisticService {

    private static final Logger log = LoggerFactory.getLogger(CtlStatisticService.class);

    private final StatisticPublisherService statisticService;
    private final StatisticMerger statisticMerger;
    private final String pathToStatisticFile;
    private final String pathToDisabledStatisticFile;

    public CtlStatisticService(String pathToStatistic,
                               String pathToDisabledStatisticFile,
                               StatisticPublisherService statisticService,
                               StatisticMerger statisticMerger) {
        this.pathToStatisticFile = pathToStatistic;
        this.pathToDisabledStatisticFile = pathToDisabledStatisticFile;
        this.statisticService = statisticService;
        this.statisticMerger = statisticMerger;
    }

    List<Path> statisticsFiles() {
        final FileStatus[] listFiles = HDFSHelper.getDirectoryContent(new Path(pathToStatisticFile).getParent());
        final List<Path> folderNames = Arrays.stream(listFiles)
                .filter(FileStatus::isFile)
                .map(FileStatus::getPath)
                .filter(file -> file.getName().startsWith("statistic_"))
                .collect(Collectors.toList());
        log.info("Statistics files are - {}", folderNames);
        return folderNames;
    }

    public void run() {
        final Map<StatisticId, String> statisticsFromFile = statisticFromFile();
        final Set<StatisticId> disabledStatistics = disabledStatisticFromFile();
        Map<StatisticId, String> statistics = statisticMerger.merge(statisticsFromFile, disabledStatistics);
        statisticService.publishStatistics(statistics);
        log.info("statistics were successfully published. deleting statistic file {}", pathToStatisticFile);
        Datamart.endAppLogDm();
        statisticsFiles().forEach(HDFSHelper::deleteFile);
    }

    private Map<StatisticId, String> statisticFromFile() {
        final List<String> statistics = HDFSHelper.readFile(pathToStatisticFile);
        log.info("read statistic file {} content: {}", pathToStatisticFile, statistics);
        final Map<StatisticId, String> parsedStatistic = new EnumMap<>(StatisticId.class);
        statistics
                .stream()
                .map(s -> s.split("=", 2))
                .map(string -> new StatisticValue(Integer.parseInt(string[0]), string[1]))
                .forEach(value -> parsedStatistic.merge(value.statisticId(), value.statisticValue(), (v1, v2) -> {
                    StatisticId statisticId = value.statisticId();
                    String exceptionMessage = String.format(
                            "Duplicate key: %s(%s) for values %s and %s", statisticId, statisticId.getCode(), v1, v2
                    );
                    throw new IllegalStateException(exceptionMessage);
                }));
        return parsedStatistic;
    }

    private Set<StatisticId> disabledStatisticFromFile() {
        final List<String> disabledStatistics = HDFSHelper.getFileContentOrEmptyList(pathToDisabledStatisticFile);
        log.info("read disabledStatistics file {} content: {}", pathToDisabledStatisticFile, disabledStatistics);
        return disabledStatistics
                .stream()
                .map(Integer::parseInt)
                .map(StatisticId::fromCode)
                .collect(Collectors.toSet());
    }

    public static class StatisticValue {
        private final StatisticId statistic;
        private final String value;

        StatisticValue(int statistic, String value) {
            this.statistic = StatisticId.fromCode(statistic);
            this.value = value;
        }

        public StatisticId statisticId() {
            return statistic;
        }

        public String statisticValue() {
            return value;
        }
    }

    public static void main(String[] args) {
        final String resultSchema = safeSystemProperty(RESULT_SCHEMA_PROPERTY);
        final String resultTable = safeSystemProperty(RESULT_TABLE_PROPERTY);

        final FullTableName datamartId = FullTableName.of(resultSchema, resultTable);
        LoggerTypeId.set(datamartId.fullTableName() + STATISTIC_PUBLISHER_POSTFIX);

        final DatamartServiceFactory serviceFactory = new DatamartServiceFactory(datamartId, datamartId + STATISTIC_PUBLISHER_POSTFIX);
        ParametersService parametersService = serviceFactory.parametersService();
        PathBuilder pathBuilder = new PathBuilder(parametersService.getEnv());
        String pathToStatistic = pathBuilder.resolveStatisticTempFile(datamartId, parametersService.ctlLoadingId());
        String pathToDisabledStatisticFile = pathBuilder.resolveDisabledStatisticTempFile(datamartId, parametersService.ctlLoadingId());

        new CtlStatisticService(
                pathToStatistic,
                pathToDisabledStatisticFile,
                serviceFactory.statisticPublisherService(),
                serviceFactory.statisticMerger()).run();
    }
}
