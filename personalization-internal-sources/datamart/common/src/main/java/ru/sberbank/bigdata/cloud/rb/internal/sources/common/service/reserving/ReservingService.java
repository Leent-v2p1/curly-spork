package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.internal.ReserveToPaMover;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_SCHEMA_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.DatamartIdResolver.RESULT_TABLE_PROPERTY;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.RESERVE_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

/**
 * Перемещает файлы из stg в pa и обновляет состояние в hive metastore,
 * используя вспомогательный класс ReserveToPaMover
 */
public class ReservingService {

    public static ReserveToPaMover initReserveToPaMover(DatamartServiceFactory serviceFactory) {
        final ParametersService parametersService = serviceFactory.parametersService();
        return ReserveToPaMover.builder(serviceFactory)
                .context(serviceFactory.sqlContext())
                .toDeleteDir(parametersService.getToDeleteDir())
                .recoveryMode(parametersService.recoveryMode())
                .recoveryDate(serviceFactory.recoveryDate())
                .isFirstLoading(parametersService.isFirstLoading())
                .naming(serviceFactory.naming(), parametersService.getEnv())
                .incrementSaveRemover(serviceFactory.incrementSaveRemover())
                .statisticToFileWriter(serviceFactory.statisticToFileWriter())
                .testPartiallyHistoryUpdate(false)//mechanism is disabled
                .buildRequiredChecker(serviceFactory.buildRequiredChecker())
                .isRepartitionNeeded(parametersService.repartition())
                .ctlDefaultStatistics(serviceFactory.ctlDefaultStatistics())
                .buildDate(serviceFactory.parametersService().ctlBuildDate())
                .statTableWriter(serviceFactory.statTableWriter())
                .create();
    }

    public static void main(String[] args) {
        String resultSchema = safeSystemProperty(RESULT_SCHEMA_PROPERTY);
        String resultTable = safeSystemProperty(RESULT_TABLE_PROPERTY);

        final FullTableName datamartId = FullTableName.of(resultSchema, resultTable);
        LoggerTypeId.set(datamartId.fullTableName() + RESERVE_POSTFIX);
        DatamartServiceFactory serviceFactory = new DatamartServiceFactory(datamartId, "reserving-service-" + datamartId);

        ReserveToPaMover reserveToPaMover = initReserveToPaMover(serviceFactory);
        reserveToPaMover.moveReserveToPa();
    }
}
