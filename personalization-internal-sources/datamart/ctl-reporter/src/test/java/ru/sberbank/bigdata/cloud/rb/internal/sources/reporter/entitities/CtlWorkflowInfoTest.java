package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CtlWorkflowInfoTest {

    @Test
    void equalsTest() {
        CtlWorkflowInfo workflowInfo = new CtlWorkflowInfo("1", "1", "1", "1");
        CtlWorkflowInfo workflowInfoSame = new CtlWorkflowInfo("1", "1", "1", "1");
        CtlWorkflowInfo workflowInfoDiff = new CtlWorkflowInfo("2", "1", "1", "1");

        assertNotEquals(workflowInfo, workflowInfoDiff);
        assertEquals(workflowInfo, workflowInfoSame);
    }

    @Test
    void hashCodeTest() {
        CtlWorkflowInfo workflowInfo = new CtlWorkflowInfo("1", "1", "1", "1");
        assertEquals(2431937, workflowInfo.hashCode());
    }
}