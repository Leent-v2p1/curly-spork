package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.WorkflowOnSupportResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.wfs.CtlCategoryResolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory.PERSONALIZATION_INTERNAL;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory.PERSONALIZATION_INTERNAL_SUPPORT;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory.PERSONALIZATION_INTERNAL_VERIFICATION;

class CtlCategoryResolverTest {

    private final WorkflowOnSupportResolver workflowOnScheduleResolver = mock(WorkflowOnSupportResolver.class);

    @Test
    void envIsTest() {
        final CtlCategoryResolver ctlCategoryResolver = new CtlCategoryResolver(Environment.PRODUCTION_TEST, workflowOnScheduleResolver);
        final CtlCategory ctlCategory = ctlCategoryResolver.resolve(null, null);
        assertEquals(PERSONALIZATION_INTERNAL_VERIFICATION, ctlCategory);
    }

    @Test
    void schemaIsOnSchedule() {

        when(workflowOnScheduleResolver.resolve(any(), any())).thenReturn(true);
        final CtlCategoryResolver ctlCategoryResolver = new CtlCategoryResolver(Environment.PRODUCTION, workflowOnScheduleResolver);
        assertEquals(PERSONALIZATION_INTERNAL_SUPPORT, ctlCategoryResolver.resolve("rb_triggers", null));
    }

    @Test
    void schemaIsNotOnSchedule() {
        when(workflowOnScheduleResolver.resolve(any(), any())).thenReturn(false);
        final CtlCategoryResolver ctlCategoryResolver = new CtlCategoryResolver(Environment.PRODUCTION, workflowOnScheduleResolver);
        assertEquals(PERSONALIZATION_INTERNAL, ctlCategoryResolver.resolve("card", null));
    }
}