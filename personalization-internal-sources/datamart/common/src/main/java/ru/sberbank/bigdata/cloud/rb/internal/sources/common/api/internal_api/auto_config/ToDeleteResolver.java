package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.HistoryUpdate;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.Increment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.HistoricalDatamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.ReplicaBasedHistoricalDatamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.ToDeleteDir;

import java.lang.annotation.Annotation;

import static java.lang.System.lineSeparator;

public class ToDeleteResolver {

    private static final Logger log = LoggerFactory.getLogger(ToDeleteResolver.class);

    public ToDeleteDir resolve(String datamartClassName) {
        final Class<?> aClass;
        try {
            aClass = Class.forName(datamartClassName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to get class " + datamartClassName, e);
        }
        final Class<?> superclass = aClass.getSuperclass();
        final Increment incremental = aClass.getAnnotation(Increment.class);
        if (incremental != null && superclass.equals(Datamart.class)) {
            log.info("{} is incremental, return NONE toDelete", datamartClassName);
            return ToDeleteDir.NONE;
        }

        final HistoryUpdate historyUpdate = aClass.getAnnotation(HistoryUpdate.class);
        if (historyUpdate != null && (superclass.equals(HistoricalDatamart.class) || superclass.equals(ReplicaBasedHistoricalDatamart.class))) {
            log.info("{} is historical, return ACTUAL toDelete", datamartClassName);
            return ToDeleteDir.ACTUAL;
        }

        final FullReplace fullReplace = aClass.getAnnotation(FullReplace.class);
        if (fullReplace != null && superclass.equals(Datamart.class)) {
            log.info("{} is datamart on full replace, return ALL toDelete", datamartClassName);
            return ToDeleteDir.ALL;
        }

        final PartialReplace partialReplace = aClass.getAnnotation(PartialReplace.class);
        if (partialReplace != null && superclass.equals(Datamart.class)) {
            log.info("{} is datamart on partial replace, return NONE toDelete", datamartClassName);
            return ToDeleteDir.NONE;
        }

        final Annotation type = ObjectUtils.firstNonNull(incremental, historyUpdate, fullReplace, partialReplace);
        String datamartStrategy = type != null
                ? type.annotationType().getSimpleName()
                : "Unresolved";

        String exceptionMessage = "Unsupported behaviour:" + lineSeparator() +
                "datamartClassName       = " + datamartClassName + lineSeparator() +
                "superClass              = " + superclass + lineSeparator() +
                "datamartSaveStrategy = " + datamartStrategy;
        throw new IllegalStateException(exceptionMessage);
    }
}
