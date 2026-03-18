package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.view;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class InstanceViewCreator {
    private static final Logger log = LoggerFactory.getLogger(InstanceViewCreator.class);
    private final DatamartContext dc;
    private final DatamartNaming naming;
    private Set<String> datamartIds;

    public InstanceViewCreator(DatamartContext dc, DatamartNaming naming, Set<String> datamartIds) {
        this.dc = dc;
        this.naming = naming;
        this.datamartIds = datamartIds;
    }

    public void run(SchemaAlias schema, List<SourcePostfix> postfixes) {
        final Map<FullTableName, List<FullTableName>> groupedByDatamart = groupByDatamartId(schema, postfixes);
        final List<FullTableName> datamartsWithError = createViews(groupedByDatamart);
        if (!datamartsWithError.isEmpty()) {
            throw new DatamartRuntimeException("There was error(s) while executing 'create view' query for datamarts: " + datamartsWithError + " check logs for detailed information");
        }
    }

    private List<FullTableName> createViews(Map<FullTableName, List<FullTableName>> groupedByDatamart) {
        final List<FullTableName> datamartsWithError = new ArrayList<>();

        for (Map.Entry<FullTableName, List<FullTableName>> entry : groupedByDatamart.entrySet()) {
            final FullTableName commonTableName = FullTableName.of(naming.sourceStageTable(entry.getKey().dbName(), entry.getKey()
                    .tableName()));
            final List<FullTableName> tables = entry.getValue();
            try {
                final List<FullTableName> tablesFiltered = tables.stream()
                        .map(t -> naming.sourceStageTable(t.dbName(), t.tableName()))
                        .map(FullTableName::of)
                        .filter(dc::exists)
                        .collect(toList());

                log.info("Found {} tables started with commonTableName {}", tablesFiltered, commonTableName);

                if (tablesFiltered.isEmpty()) {
                    continue;
                }

                final List<StructType> structTypes = tablesFiltered
                        .stream()
                        .map(dc::sourceTable)
                        .map(Dataset<Row>::schema)
                        .collect(Collectors.toList());

                checkTypes(structTypes);

                final String asSelect = tablesFiltered.stream()
                        .map(table -> " SELECT " + getColumnNames(structTypes, table.fullTableName()) + " FROM " + table.fullTableName())
                        .collect(Collectors.joining(" UNION ALL"));

                dc.context().sql("DROP VIEW IF EXISTS " + commonTableName.fullTableName());
                final String sqlCreateView = "CREATE VIEW " + commonTableName.fullTableName() + " AS " + asSelect;
                System.out.println("sqlCreateView = " + sqlCreateView);
                dc.context().sql(sqlCreateView);
            } catch (Exception e) {
                log.error("Error when creating the view for '{}'", commonTableName, e);
                datamartsWithError.add(commonTableName);
            }
        }
        return datamartsWithError;
    }

    protected String getColumnNames(List<StructType> structTypes, String fullTableName) {
        return String.join(", ", structTypes.get(0).fieldNames());
    }

    private void checkTypes(List<StructType> structTypes) {
        for (int i = 0; i < structTypes.size() - 1; i++) {
            SparkSQLUtil.checkTypesExactlyMatch(structTypes.get(i), structTypes.get(i + 1));
        }
    }

    private Map<FullTableName, List<FullTableName>> groupByDatamartId(SchemaAlias schemaAlias, List<SourcePostfix> postfixes) {
        return datamartIds.stream()
                .map(FullTableName::of)
                .filter(datamartId -> schemaAlias.name().equalsIgnoreCase(datamartId.dbName()))
                .filter(datamartId -> hasAnyPostfix(datamartId, postfixes))
                .collect(groupingBy(datamartId -> findCommonTableName(datamartId, postfixes)));
    }

    private boolean hasAnyPostfix(FullTableName datamartId, List<SourcePostfix> postfixes) {
        return postfixes.stream().anyMatch(postfix -> datamartId.tableName().endsWith(postfix.getPostfix()));
    }

    private FullTableName findCommonTableName(FullTableName datamartId, List<SourcePostfix> postfixes) {
        final Optional<String> commonTableName = postfixes.stream()
                .map(postfix -> removePostfix(datamartId, postfix))
                .min(comparing(String::length));
        return FullTableName.of(datamartId.dbName(), commonTableName.orElseThrow(() -> new IllegalStateException("Can't find common table name for table: " + datamartId)));
    }

    private String removePostfix(FullTableName datamartId, SourcePostfix postfix) {
        return datamartId.tableName().replaceAll("_" + postfix.getPostfix() + "$", "");
    }
}
