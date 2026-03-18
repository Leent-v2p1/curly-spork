package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.ReplicaContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.test_api.CsvDatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.test_api.DatamartTest;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.Month;
import java.util.Collections;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils.row;

class RecoveredReplicaTableCreatorTest extends DatamartTest {

    private final ReplicaContext replicaContext = mock(ReplicaContext.class);
    private final StructType resultStructType = new StructType()
            .add("id", IntegerType)
            .add("field1", StringType)
            .add("field2", IntegerType)
            .add("field3", DoubleType)
            .add("ctl_validfrom", TimestampType)
            .add("ctl_validto", TimestampType)
            .add("ctl_action", StringType);

    @Test
    @DisplayName("создаёт реплику за день")
    void testBuildReplica() {
        final CsvDatamartContext context = CsvDatamartContext.builder(sqlContext)
                .csvDir("history/history_stage")
                .tableCsv("hist_0_20180102000000", true)
                .tableCsv("snp", true)
                .expectedDatamartCsv("result", resultStructType)
                .create();

        when(replicaContext.fullNamedTable(any())).then(invocation -> context.fullNamedTable(((FullTableName) invocation.getArguments()[0]).fullTableName()));
        when(replicaContext.schemaTables(any())).thenReturn(Collections.singletonList("hist_0_20180102000000"));

        final RecoveredReplicaTableCreator recoveredReplicaTableCreator = new RecoveredReplicaTableCreator(
                replicaContext,
                FullTableName.of("test.snp"),
                FullTableName.of("test.hist"),
                LocalDate.of(2018, Month.JANUARY, 2)
        );

        final Dataset<Row> actual = recoveredReplicaTableCreator.buildReplica();
        assertDataFramesEqual(actual, context.expectedDatamart());
    }

    @Test
    @DisplayName("создаёт реплику за указанный день, для получения имени использует специальный алгоритм для ODS источников")
    void testBuildOdsReplice() {
        CsvDatamartContext context = CsvDatamartContext.builder(sqlContext).create();

        final FullTableName snp = FullTableName.of("internal_mdm_mdm.snp_part");
        dropAndCreateSchema(snp.dbName());
        final Dataset<Row> snpDataFrame = createSnpDataFrame();
        context.saveTemp(snpDataFrame, snp.fullTableName());

        final FullTableName hst = FullTableName.of("internal_mdm_mdm_hist.hist_part");
        dropAndCreateSchema(hst.dbName());
        final Dataset<Row> hstDataFrame = createHstDataFrame();
        context.saveTemp(hstDataFrame, hst.fullTableName());

        when(replicaContext.fullNamedTable(any())).then(invocation -> sqlContext.table(((FullTableName) invocation.getArguments()[0]).fullTableName()));
        when(replicaContext.schemaTables(any())).thenReturn(Collections.singletonList("hist_part"));

        final RecoveredReplicaTableCreator recoveredReplicaTableCreator = new RecoveredReplicaTableCreator(
                replicaContext,
                snp,
                hst,
                LocalDate.of(2018, Month.NOVEMBER, 24)
        );

        final Dataset<Row> actual = recoveredReplicaTableCreator.buildReplica();
        assertDataFramesEqual(actual, hstDataFrame);
    }

    private Dataset<Row> createSnpDataFrame() {
        final StructType snpStructType = new StructType()
                .add("id", IntegerType)
                .add("ctl_validfrom", TimestampType)
                .add("ctl_action", StringType);
        return createDF(snpStructType, row(1, Timestamp.valueOf(LocalDate.of(2018, Month.NOVEMBER, 25).atStartOfDay()), "I"));
    }

    private Dataset<Row> createHstDataFrame() {
        final StructType hstStructType = new StructType()
                .add("id", IntegerType)
                .add("ctl_validfrom", TimestampType)
                .add("ctl_validto", TimestampType)
                .add("ctl_action", StringType);
        final Timestamp ctlValidFrom = Timestamp.valueOf(LocalDate.of(2018, Month.NOVEMBER, 24).atStartOfDay());
        final Timestamp ctlValidTo = Timestamp.valueOf(LocalDate.of(2018, Month.NOVEMBER, 25).atStartOfDay());
        return createDF(hstStructType, row(1, ctlValidFrom, ctlValidTo, "I"));
    }

    @Test
    @DisplayName("падает с ошибкой если на указанный день нет таблицы реплики в исторической части")
    void testNoHistoryTable() {
        final CsvDatamartContext context = CsvDatamartContext.builder(sqlContext)
                .csvDir("history/no_history_table")
                .tableCsv("snp", true)
                .expectedDatamartCsv("result", resultStructType)
                .create();

        when(replicaContext.fullNamedTable(any())).then(invocation -> context.fullNamedTable(((FullTableName) invocation.getArguments()[0]).fullTableName()));
        final RecoveredReplicaTableCreator recoveredReplicaTableCreator = new RecoveredReplicaTableCreator(
                replicaContext,
                FullTableName.of("test.snp"),
                FullTableName.of("test.hist"),
                LocalDate.of(2018, Month.JANUARY, 2));
        final IllegalStateException exception = assertThrows(IllegalStateException.class, recoveredReplicaTableCreator::buildReplica);
        assertEquals("No history table test.hist for day 2018-01-02", exception.getMessage());
    }
}
