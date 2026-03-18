package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write;

import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;

import javax.xml.bind.JAXB;
import java.io.ByteArrayInputStream;

public class WorkflowAppXmlUnMarshaller {

    public WORKFLOWAPP unmarshall(String content) {
        final String transformedXml = transformXml(content);
        return JAXB.unmarshal(new ByteArrayInputStream(transformedXml.getBytes()), WORKFLOWAPP.class);
    }

    private String transformXml(String xml) {
        return xml.replace("uri:oozie:workflow:0.4", "uri:oozie:workflow:0.5");
    }
}
