package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Class deserialize WORKFLOWAPP files and put xml representation in the specific directory
 */
public class WorkflowWriter {
    static final String WORKFLOW_NAME = "workflow.xml";
    private static final Logger log = LoggerFactory.getLogger(WorkflowWriter.class);

    public void writeWorkflow(Path fullPath, WORKFLOWAPP workflowContent) {
        String workflowAppXml = new WorkflowAppXmlMarshaller().marshall(workflowContent);
        log.info("Writing workflow to: {}", fullPath);
        try {
            Files.createDirectories(fullPath);
            Path resultPath = fullPath.resolve(WORKFLOW_NAME);
            FileUtils.writeFile(resultPath, workflowAppXml.getBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
