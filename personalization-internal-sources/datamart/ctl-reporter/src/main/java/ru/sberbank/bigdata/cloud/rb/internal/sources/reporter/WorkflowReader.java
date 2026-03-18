package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter;

import ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities.CtlWorkflowInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WorkflowReader {

    public static final String WORKFLOW_IS_USED_IN_REPORT_FLAG = "1";

    public Map<Integer, CtlWorkflowInfo> readWorkflowsFromFile(Path path) throws IOException {
        final Path workflowFile = getWorkflowFile(path);
        try (Stream<String> lines = Files.lines(workflowFile)) {
            final Map<Integer, CtlWorkflowInfo> collectedWorkflows = lines
                    .skip(1)   // header
                    .map(line -> line.split(";"))
                    .map(wf -> new CtlWorkflowInfo(wf[0], wf[1], wf[2], wf[3]))
                    .filter(wfInfo -> wfInfo.isUsed.equals(WORKFLOW_IS_USED_IN_REPORT_FLAG))
                    .collect(Collectors.toMap(wfInfoFiltered -> Integer.valueOf(wfInfoFiltered.id), wfInfoFiltered -> wfInfoFiltered));
            if (collectedWorkflows.isEmpty()) {
                throw new IllegalStateException("There's no worflows in " + workflowFile + ", " +
                        "probably none of them has '" + WORKFLOW_IS_USED_IN_REPORT_FLAG + "' in 'is_used' field");
            }
            return collectedWorkflows;
        }
    }

    Path getWorkflowFile(Path path) throws IOException {
        try (Stream<Path> filesStream = Files.walk(path, 1)) {
            return filesStream
                    .filter(file -> !file.toFile().isDirectory())
                    .filter(file -> file.toString().endsWith("workflows.csv"))
                    .max(Comparator.naturalOrder())
                    .orElseThrow(() -> new IllegalStateException("There is no file with workflows in " + path.toString()));
        }
    }
}
