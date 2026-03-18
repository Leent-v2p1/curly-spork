package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.view;

import freemarker.template.Template;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper;

import java.util.Collections;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.getSystemProperty;

/**
 * Класс для создания вьюхи
 * Created by sbt-buylin-ma on 21.02.2018.
 */
public class DatamartCreateView {
    private final SparkSession sqlContext;
    private final String viewName;
    private final String sourceTable;
    private final String sqlTemplate;

    protected DatamartCreateView(SparkSession sqlContext, String viewName, String sourceTable, String sqlTemplate) {
        this.sqlContext = sqlContext;
        this.viewName = viewName;
        this.sourceTable = sourceTable;
        this.sqlTemplate = sqlTemplate;
    }

    public void run() {
        sqlContext.sql("drop view if exists " + viewName);
        String sql = getSql();
        sqlContext.sql("create view " + viewName + " AS " + sql);
    }

    private String getSql() {
        final Template template = FreemarkerHelper.getTemplate(sqlTemplate);
        return FreemarkerHelper.printTemplate(template, Collections.singletonMap("source", sourceTable));
    }

    public static void main(String[] args) {
        final FullTableName viewId = FullTableName.of(getSystemProperty("spark.view.name"));
        final DatamartServiceFactory serviceFactory = new DatamartServiceFactory(viewId, "create-view-" + viewId);
        final DatamartProperties properties = new DatamartProperties(viewId);
        final DatamartNaming naming = serviceFactory.naming();
        final String viewSourceTable = properties.getViewSourceTable();

        final DatamartCreateView datamartCreateView =
                new DatamartCreateView(serviceFactory.sqlContext(),
                        naming.fullTableName(),
                        naming.target(viewSourceTable),
                        properties.getViewSqlTemplate()
                );
        datamartCreateView.run();
    }
}
