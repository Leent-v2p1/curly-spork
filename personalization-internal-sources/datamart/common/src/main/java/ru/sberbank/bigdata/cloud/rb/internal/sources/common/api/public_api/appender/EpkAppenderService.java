package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.MdmAppenderStageNames.EpkSystemsStageTables;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.appender.ClientIdAppender;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.appender.epk.EpkIdFromEpkSystem;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.Optional;

/**
 * Service appends client_epk field to datamart - id of client in EPK system
 * Append step appends column with client_dk to datamart during build of this datamart
 */
public class EpkAppenderService {

    private static final Logger log = LoggerFactory.getLogger(EpkAppenderService.class);

    private final DatamartContext datamartContext;
    private final Optional<SourcePostfix> sourcePostfix;

    public EpkAppenderService(DatamartContext datamartContext,
                              Optional<SourcePostfix> sourcePostfix) {
        this.datamartContext = datamartContext;
        this.sourcePostfix = sourcePostfix;
    }

    /**
     * Универсальный метод по получению EPK_ID аппендера для любой системы, использующий таблицы из EpkSystemsStageTables
     */
    public ClientIdAppender epkIdFromEpkSystemFor(EpkSystemsStageTables system) {
        final FullTableName stage = system.getTable(sourcePostfix);
        log.info("resolved stage table name {}", stage);
        return new EpkIdFromEpkSystem(datamartContext.sourceTable(stage));
    }
}
