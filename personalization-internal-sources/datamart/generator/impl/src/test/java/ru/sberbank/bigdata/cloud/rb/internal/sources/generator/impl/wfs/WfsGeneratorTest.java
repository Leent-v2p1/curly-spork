package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.wfs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DailySourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.MonthlySourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourceInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.test_api.DatamartTest;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.wfs.CtlCategoryResolver;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.CollectionUtils.setOf;

class WfsGeneratorTest extends DatamartTest {

    private static final String ALIAS = "schema_alias";
    private static final FullTableName TEST_DATAMART = FullTableName.of("custom_rb_test.datamart");

    private final CtlCategoryResolver ctlCategoryResolver = mock(CtlCategoryResolver.class);
    private final CtlWorkflowGenerator workflowGenerator = mock(CtlWorkflowGenerator.class);

    @BeforeEach
    void setUp() {
        when(ctlCategoryResolver.resolve(any(), any())).thenReturn(CtlCategory.PERSONALIZATION_INTERNAL);
        when(workflowGenerator.generateHeader(eq(ALIAS))).thenReturn("header");
    }

    @Test
    void dailyAndMonthly() {
        final WfsGenerator wfsGenerator = initGenerator();

        final Map<String, String> result = wfsGenerator.generate();

        assertThat(result.entrySet(), hasSize(1));
        final String content = result.get(ALIAS);
        assertEquals("header|daily-wf-content||monthly-wf-content|", content);
    }

    @Test
    void multipleInstances() {
        final WfsGenerator wfsGenerator = initGeneratorForInstances();

        final Map<String, String> result = wfsGenerator.generate();

        assertThat(result.entrySet(), hasSize(1));
        final String content = result.get(ALIAS);
        assertEquals("header|instance1||instance2|", content);
    }

    @AfterEach
    void tearDown() {
        SourceInstance.INSTANCES_FOR_SOURCE.remove(ALIAS);
    }

    private WfsGenerator initGenerator() {
        final FullTableName monthlyDatamart = FullTableName.of("custom_rb_test.datamart_month");
        final Set<String> tables = setOf(TEST_DATAMART.fullTableName(), monthlyDatamart.fullTableName());
        final DatamartProperties propertiesForDaily = mockPropertiesService(TEST_DATAMART.tableName(), 1, TEST_DATAMART.dbName());
        final DatamartProperties propertiesForMonthly = mockPropertiesService(monthlyDatamart.tableName(), 2, monthlyDatamart.dbName());

        final DailySourcePostfix dailySourcePostfix = new DailySourcePostfix();
        final MonthlySourcePostfix monthlySourcePostfix = new MonthlySourcePostfix();
        mockGeneratorCall(workflowGenerator, SourceInstance.getSourceInstance(dailySourcePostfix), "|daily-wf-content|");
        mockGeneratorCall(workflowGenerator, SourceInstance.getSourceInstance(monthlySourcePostfix), "|monthly-wf-content|");
        SourceInstance.INSTANCES_FOR_SOURCE.put(ALIAS, Arrays.asList(dailySourcePostfix, monthlySourcePostfix));
        final WfsGenerator wfsGenerator = spy(new WfsGenerator(
                dmId -> dmId.equals(TEST_DATAMART.fullTableName()) ? propertiesForDaily : propertiesForMonthly,
                tables,
                Collections.emptyList(),
                Environment.PRODUCTION,
                workflowGenerator,
                ctlCategoryResolver
        ));
        doReturn(ALIAS).when(wfsGenerator).getAlias(anyString());
        return wfsGenerator;
    }

    private WfsGenerator initGeneratorForInstances() {
        final Set<String> tables = setOf(TEST_DATAMART.fullTableName());
        final DatamartProperties propertiesForDaily = mockPropertiesService(TEST_DATAMART.tableName(), 1, TEST_DATAMART.dbName());

        final TestSourcePostfix instance1 = new TestSourcePostfix("instance1");
        final SourceInstance sourceInstance1 = SourceInstance.getSourceInstance(instance1);
        mockGeneratorCall(workflowGenerator, sourceInstance1, "|instance1|");

        final TestSourcePostfix instance2 = new TestSourcePostfix("instance2");
        final SourceInstance sourceInstance2 = SourceInstance.getSourceInstance(instance2);
        mockGeneratorCall(workflowGenerator, sourceInstance2, "|instance2|");

        SourceInstance.INSTANCES_FOR_SOURCE.put(ALIAS, Arrays.asList(instance1, instance2));

        final WfsGenerator wfsGenerator = spy(new WfsGenerator(
                dmId -> propertiesForDaily,
                tables,
                Collections.emptyList(),
                Environment.PRODUCTION,
                workflowGenerator,
                ctlCategoryResolver
        ));
        doReturn(ALIAS).when(wfsGenerator).getAlias(anyString());
        return wfsGenerator;
    }

    private void mockGeneratorCall(CtlWorkflowGenerator workflowGenerator,
                                   SourceInstance sourceInstance,
                                   String content) {
        when(workflowGenerator.generateWorkflow(
                eq(WfsGeneratorTest.ALIAS),
                eq(ScheduleType.NONE),
                eq(CtlCategory.PERSONALIZATION_INTERNAL),
                eq(sourceInstance))
        ).thenReturn(content);
    }

    private DatamartProperties mockPropertiesService(String targetTable, Integer ctlEntityId, String schema) {
        DatamartProperties propertiesDaily = mock(DatamartProperties.class);
        when(propertiesDaily.getTargetSchema()).thenReturn(schema);
        when(propertiesDaily.getTargetTable()).thenReturn(targetTable);
        when(propertiesDaily.getCtlEntityId()).thenReturn(Optional.of(ctlEntityId));
        return propertiesDaily;
    }

    static class TestSourcePostfix implements SourcePostfix {

        private String instance;

        public TestSourcePostfix(String instance) {
            this.instance = instance;
        }

        @Override
        public String getPostfix() {
            return "daily_" + instance;
        }

        @Override
        public String getPath() {
            return "daily_" + instance;
        }
    }
}