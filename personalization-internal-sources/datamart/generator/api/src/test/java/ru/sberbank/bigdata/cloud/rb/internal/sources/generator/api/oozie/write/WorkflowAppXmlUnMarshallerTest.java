package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import javax.xml.bind.JAXBException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WorkflowAppXmlUnMarshallerTest {

    @Test
    void unmarshall() throws JAXBException {
        final String content = FileUtils.readAsString(Paths.get("src/test/resources/unmarshaller/test-workflow.xml"));
        final WorkflowAppXmlUnMarshaller workflowAppXmlUnMarshaller = new WorkflowAppXmlUnMarshaller();
        final WORKFLOWAPP workflowapp = workflowAppXmlUnMarshaller.unmarshall(content);
        assertNotNull(workflowapp);
        assertEquals(2, workflowapp.getDecisionOrForkOrJoin().size());
    }
}