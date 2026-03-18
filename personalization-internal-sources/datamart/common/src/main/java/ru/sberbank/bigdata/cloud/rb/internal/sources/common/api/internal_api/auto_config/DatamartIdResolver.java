package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

/**
 * Получает id витрины из аннотации DatamartRef, либо из свойства указанного в параметре 'useSystemPropertyToGetId'
 */
public class DatamartIdResolver {

    public static final String RESULT_TABLE_PROPERTY = "spark.result.table";
    public static final String RESULT_SCHEMA_PROPERTY = "spark.result.schema";

    private static final Logger log = LoggerFactory.getLogger(DatamartIdResolver.class);

    public FullTableName resolveId(Class<? extends Datamart> datamartClass) {

        Datamart.startAppLogDm();

        final String dmName = datamartClass.getName();
        log.info("resolving datamart id for {}", dmName);
        final boolean annotationPresent = datamartClass.isAnnotationPresent(DatamartRef.class);
        if (!annotationPresent) {
            throw new IllegalStateException(dmName + " doesn't has an annotation 'DatamartRef'");
        }
        final DatamartRef annotation = datamartClass.getAnnotation(DatamartRef.class);
        final boolean useSystemPropertyToGetId = annotation.useSystemPropertyToGetId();

        if (useSystemPropertyToGetId) {
            final String schemaName = SysPropertyTool.safeSystemProperty(RESULT_SCHEMA_PROPERTY);
            final String tableName = SysPropertyTool.safeSystemProperty(RESULT_TABLE_PROPERTY);
            return FullTableName.of(schemaName, tableName);
        } else {
            final String datamartId = annotation.id();
            log.info("resolve datamart id with value {} from key id in annotation DatamartRef", datamartId);
            return FullTableName.of(datamartId);
        }
    }
}
