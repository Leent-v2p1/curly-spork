package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.change_description.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.ReplicaBasedStagingDatamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.StagingDatamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.spark.sql.types.DataTypes.NullType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFixType.SCHEMA_CHANGE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils.structField;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.isTableExists;

public class DataFixAPI {

    private static final Logger log = LoggerFactory.getLogger(DataFixAPI.class);

    private final List<DataFix> actions = new ArrayList<>();
    private final SparkSession context;
    private final DatamartNaming naming;
    private final Function<List<DataFix>, DataFixRunner> runnerCreator;

    public DataFixAPI(SparkSession context, DatamartNaming naming, DatamartServiceFactory dsf) {
        this.context = context;
        this.naming = naming;
        this.runnerCreator = dsf::datafixRunner;
    }

    public void addColumnIfMissing(String columnName, DataType type) {
        final String tableName = naming.fullTableName();
        final List<String> paFields = Arrays.asList(context.table(tableName).schema().fieldNames());
        if (paFields.contains(columnName)) {
            log.info("Table {} already contains column {}. AddColumn is not applied.", tableName, columnName);
            return;
        }

        if (isActionExists(columnName)) {
            log.info("Actions of {} already contains column {}. AddColumn is not applied.", tableName, columnName);
            return;
        }

        StructField structField = structField(columnName, type);
        AddColumn addColumn = new AddColumn(structField);

        actions.add(addColumn);
    }

    public void deleteColumnIfExist(String columnName) {
        final String tableName = naming.fullTableName();
        final List<String> paFields = Arrays.asList(context.table(tableName).schema().fieldNames());
        if (!paFields.contains(columnName)) {
            log.info("Table {} does not contain column {}. DeleteColumn is not applied.", tableName, columnName);
            return;
        }

        if (isActionExists(columnName)) {
            log.info("Actions of {} already contains column {}. DeleteColumn is not applied.", tableName, columnName);
            return;
        }
        StructField structField = structField(columnName, NullType);
        RemoveColumn removeColumn = new RemoveColumn(structField);

        actions.add(removeColumn);
    }

    public void castColumnIfNotCasted(String columnName, DataType toType) {

        final String tableName = naming.fullTableName();
        final List<StructField> paFields = Arrays.asList(context.table(tableName).schema().fields());
        Optional<StructField> column = paFields.stream()
                .filter(field -> field.name().equals(columnName))
                .findFirst();
        if (!column.isPresent()) {
            log.info("Table {} does not contain column {}. CastColumn can not be applied.", tableName, columnName);
            return;
        }
        StructField from = column.get();
        if (from.dataType().equals(toType)) {
            log.info("Table {} already contains column {} of type {}. CastColumn is not applied.", tableName, columnName, toType);
            return;
        }
        if (isActionExists(columnName)) {
            log.info("Actions of {} already contains column {}. CastColumn is not applied.", tableName, columnName);
            return;
        }
        StructField to = structField(columnName, toType);
        ChangeColumnType changeColumnType = new ChangeColumnType(from, to);

        actions.add(changeColumnType);
    }

    public void renameColumnIfExist(String fromName, String toName) {
        final String tableName = naming.fullTableName();
        if (fromName.equals(toName)) {
            log.info("RenameColumn can not be applied for {} table because column names are equal ({}) ", tableName, fromName);
            return;
        }

        final List<StructField> paFields = Arrays.asList(context.table(tableName).schema().fields());
        Optional<StructField> column = paFields.stream()
                .filter(field -> field.name().equals(fromName))
                .findFirst();
        if (!column.isPresent()) {
            log.info("Table {} does not contain column {}. RenameColumn can not be applied.", tableName, fromName);
            return;
        }

        boolean toNameIsAvailable = paFields.stream()
                .noneMatch(field -> field.name().equals(toName));
        if (!toNameIsAvailable) {
            log.info("Table {} does already contain column {}. RenameColumn can not be applied.", tableName, toName);
            return;
        }
        StructField from = column.get();

        if (isActionExists(fromName)) {
            log.info("Actions of {} already contains column {}. RenameColumn is not applied.", tableName, fromName);
            return;
        }
        StructField to = structField(toName, from.dataType());
        RenameColumn renameColumnType = new RenameColumn(from, to);

        actions.add(renameColumnType);
    }

    public void run(Datamart datamart) {
        final Class<?> datamartClass = datamart.getClass();
        final Class superclass = datamartClass.getSuperclass();
        if (superclass.equals(StagingDatamart.class) || superclass.equals(ReplicaBasedStagingDatamart.class)) {
            log.info("Skip datafixes for stage {}", datamartClass);
            return;
        }

        final String tableName = naming.fullTableName();
        if (getActions().isEmpty() || !isTableExists(context, FullTableName.of(tableName))) {
            log.info("There are no need to run datafixes to {} datamart", tableName);
            return;
        }

        log.info("Initialize {} datafixes for {}", getActions().size(), datamartClass);
        dataFixRunner().run(datamart);
    }

    DataFixRunner dataFixRunner() {
        return runnerCreator.apply(getActions());
    }

    List<DataFix> getActions() {
        return actions;
    }

    private boolean isActionExists(String columnName) {
        return actions.stream()
                .filter(a -> a.type() == SCHEMA_CHANGE)
                .map(ChangeAction.class::cast)
                .anyMatch(action ->
                        (action.to() != null && columnName.equals(action.to().name()))
                                || (action.from() != null && columnName.equals(action.from().name()))
                );
    }
}
