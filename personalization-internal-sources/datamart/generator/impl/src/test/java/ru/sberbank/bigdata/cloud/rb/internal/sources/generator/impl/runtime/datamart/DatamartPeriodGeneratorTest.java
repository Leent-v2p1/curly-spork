package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.datamart;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper;

import java.time.LocalDate;
import java.time.Period;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DatamartPeriodGeneratorTest {

    @Test
    void containsStartAndEnd() {
        final LocalDate startDate = LocalDate.of(2015, 3, 1);
        final LocalDate current = LocalDate.of(2018, 8, 1);
        DatamartPeriodGenerator generator = createGenerator(startDate, current);

        final String oozieWf = generator.generate();
        assertTrue(oozieWf.contains("2015-04-01"));
        assertTrue(oozieWf.contains("2018-07-01"));
        assertFalse(oozieWf.contains("2018-08-01"));
    }

    @Test
    void containsAllActions() {
        final LocalDate start = LocalDate.of(2015, 4, 1);
        final LocalDate current = LocalDate.of(2018, 8, 1);

        final DatamartPeriodGenerator generator = createGenerator(start, current);

        final String workflow = generator.generate();

        final Period period = Period.between(start, current);
        final int monthsBetween = period.getMonths() + period.getYears() * 12;

        LocalDate currentMonth = start;
        for (int i = 0; i < monthsBetween; i++) {
            String monthStr = currentMonth.format(DateHelper.MONTH_FORMAT);
            assertTrue(workflow.contains(monthStr), "Month " + monthStr);
            currentMonth = currentMonth.plusMonths(1);
        }
    }

    private DatamartPeriodGenerator createGenerator(LocalDate startDate, LocalDate currentDate) {
        FullTableName tableName = FullTableName.of("custom_rb_card", "tbl");
        return new DatamartPeriodGenerator(tableName, createDatamartServiceFactory(currentDate), "path") {
            @Override
            protected LocalDate getStartDate(boolean firstBuild, LocalDate buildDate) {
                return startDate;
            }
        };
    }

    private DatamartServiceFactory createDatamartServiceFactory(LocalDate currentDate) {
        final DatamartServiceFactory serviceFactory = mock(DatamartServiceFactory.class);
        ParametersService parametersService = mock(ParametersService.class);
        doReturn(parametersService).when(serviceFactory).parametersService();
        when(parametersService.getEnv()).thenReturn(Environment.PRODUCTION);
        when(parametersService.ctlBuildDate()).thenReturn(currentDate);
        when(parametersService.isFirstLoading()).thenReturn(true);
        return serviceFactory;
    }
}
