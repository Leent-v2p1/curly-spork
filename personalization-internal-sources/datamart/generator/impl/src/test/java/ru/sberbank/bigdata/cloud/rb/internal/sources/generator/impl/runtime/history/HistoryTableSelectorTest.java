package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.time.LocalDate;
import java.time.Month;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by sbt-buylin-ma on 15.03.2017.
 */
class HistoryTableSelectorTest {

    @Test
    void testFindNearestLeftHistoryTable() {
        final DatamartContext datamartContext = mock(DatamartContext.class);
        final List<String> tablesInDb = asList("z_account_hst_1_2480539600", "z_account_hst_1_480539600", "z_account_hst_1_1480539600");
        when(datamartContext.schemaTables("db_name"))
                .thenReturn(tablesInDb);

        HistoryTableSelector historyTableSelector = new HistoryTableSelector(datamartContext, "db_name.z_account_hst");

        FullTableName result = historyTableSelector.selectHistoryTable(LocalDate.of(2018, Month.FEBRUARY, 8));
        assertEquals("db_name.z_account_hst_1_1480539600", result.fullTableName());
    }

    @Test
    void testFindNearestLeftHistoryTable_debug() {
        System.setProperty("spark.history.stage.debug", "true");
        final DatamartContext datamartContext = mock(DatamartContext.class);
        final List<String> tablesInDb = asList("z_account_hst_1_20170823000000", "z_account_hst_1_1486846800", "z_account_hst_1_1480539600");
        when(datamartContext.schemaTables("db_name"))
                .thenReturn(tablesInDb);

        HistoryTableSelector historyTableSelector = new HistoryTableSelector(datamartContext, "db_name.z_account_hst");

        FullTableName result = historyTableSelector.selectHistoryTable(LocalDate.of(2018, Month.FEBRUARY, 8));
        assertEquals("db_name.z_account_hst_1_20170823000000", result.fullTableName());

        System.clearProperty("history.stage.debug");
    }
}
