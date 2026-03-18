package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getCtlWorkflowKeytabHolder;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getCtlWorkflowName;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getCtlWorkflowUserHolder;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getWfsFileName;

class CtlWorkflowNameTest {

    @Test
    void testGetCtlWorkflowName() {
        final String actual = getCtlWorkflowName(Environment.PRODUCTION, "erib", "daily");
        final String expected = "masspers-{{common.ctl_v5.workflow.prefix}}-erib-daily-datamarts";
        assertEquals(expected, actual);

        final String actualTest = getCtlWorkflowName(Environment.PRODUCTION_TEST, "erib", "daily");
        final String expectedTest = "masspers-{{common.ctl_v5.workflow.prefix}}-test-erib-daily-datamarts";
        assertEquals(expectedTest, actualTest);
    }

    @Test
    void testGetWfsFileName() {
        final String actualProd = getWfsFileName(Environment.PRODUCTION, "erib");
        final String expectedProd = "erib-wfs.yaml";
        assertEquals(expectedProd, actualProd);

        final String actualTest = getWfsFileName(Environment.PRODUCTION_TEST, "way4");
        final String expectedTest = "test-way4-wfs.yaml";
        assertEquals(expectedTest, actualTest);
    }

    @Test
    void testCtlWorkflowUserHolder() {
        final String actual = getCtlWorkflowUserHolder("erib");
        final String expected = "{{security.erib.username}}";
        assertEquals(expected, actual);

    }

    @Test
    void testCtlWorkflowKeytabHolder() {
        final String actual = getCtlWorkflowKeytabHolder("rb_triggers");
        final String expected = "{{security.rb_triggers.keytab.path}}";
        assertEquals(expected, actual);
    }
}