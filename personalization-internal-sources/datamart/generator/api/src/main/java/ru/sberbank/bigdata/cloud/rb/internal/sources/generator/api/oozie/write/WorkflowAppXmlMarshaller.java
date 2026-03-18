package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;

public class WorkflowAppXmlMarshaller {

    public String marshall(WORKFLOWAPP workflowapp) {
        StringWriter writer = new StringWriter();
        try {
            JAXBContext context = JAXBContext.newInstance(WORKFLOWAPP.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.marshal(workflowapp, writer);
        } catch (JAXBException e) {
            throw new DatamartRuntimeException(e);
        }
        return transformXml(writer.toString());
    }

    /**
     * Generated xml is valid, but HUE doesn't support it correctly, so it additionally should be transformed to HUE-correct xml.
     */
    private String transformXml(String xml) {
        return xml.replaceAll("xmlns:hive=\"uri:oozie:hive-action:0.5\"", "")
                .replaceAll("xmlns:spark=\"uri:oozie:spark-action:0.1\"", "")
                .replaceAll("xmlns:shell=\"uri:oozie:shell-action:0.3\"", "")
                .replaceAll("spark:", "")
                .replaceAll("hive:", "")
                .replaceAll("shell:", "")
                .replaceAll("<spark>", "<spark xmlns=\"uri:oozie:spark-action:0.2\">")
                .replaceAll("<hive>", "<spark xmlns=\"uri:oozie:hive-action:0.5\">")
                .replaceAll("<shell>", "<shell xmlns=\"uri:oozie:shell-action:0.3\">");
    }
}
