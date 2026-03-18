package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.copier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.WorkflowNamePrefixResolver;

import java.util.HashMap;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.PRODUCTION_PARENT_APP_PATH;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.PRODUCTION_WORKFLOWS;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.UTILS_DIR;

public class SubWfTransformer {
    private static final Logger log = LoggerFactory.getLogger(SubWfTransformer.class);
    private static HashMap<String, String> typeToReplace = new HashMap<>();

    static {
        typeToReplace.put("sub-workflow", "app-path");
        typeToReplace.put("java", "file");
        typeToReplace.put("spark", "jar");
    }

    private final String workflowPath;
    private final String utilsDir;
    private final String parentAppPath;
    private final Environment env;

    public SubWfTransformer(String workflowPath, String utilsDir, String parentAppPath, Environment env) {
        this.workflowPath = workflowPath;
        this.utilsDir = utilsDir;
        this.parentAppPath = parentAppPath;
        this.env = env;
    }

    public void replaceWfAppName(Document xml) {
        final Node workflowAppTag = xml.getElementsByTagName("workflow-app").item(0);
        final Node wfAppName = workflowAppTag.getAttributes().getNamedItem("name");
        final String envShortName = new WorkflowNamePrefixResolver(env).resolve();
        wfAppName.setTextContent(envShortName + "-" + wfAppName.getTextContent());
    }

    public void replacePaths(Document xml) {
        NodeList actions = xml.getElementsByTagName("action");
        for (int indexOfAction = 0; indexOfAction < actions.getLength(); indexOfAction++) {
            Node action = actions.item(indexOfAction);

            Node currentAction = action.getFirstChild();
            do {
                String actionType = currentAction.getNodeName();
                if (typeToReplace.keySet().contains(actionType)) {
                    NodeList childNodes = currentAction.getChildNodes();
                    iterateThroughProperties(actionType, childNodes);
                }
            } while ((currentAction = currentAction.getNextSibling()) != null);
        }
    }

    private void iterateThroughProperties(String actionType, NodeList childNodes) {
        String variableToReplace = typeToReplace.get(actionType);
        for (int indexOfParameter = 0; indexOfParameter < childNodes.getLength(); indexOfParameter++) {
            Node parameter = childNodes.item(indexOfParameter);
            if (parameter.getNodeName().equals(variableToReplace)) {
                String content = parameter.getTextContent();
                log.warn("Old value for {} is {}", env, content);
                if (content.contains(PRODUCTION_WORKFLOWS)) {
                    content = content.replace(PRODUCTION_WORKFLOWS, workflowPath);
                } else if (content.contains(UTILS_DIR)) {
                    content = content.replace(UTILS_DIR, utilsDir);
                } else if (content.contains(PRODUCTION_PARENT_APP_PATH)) {
                    content = content.replace(PRODUCTION_PARENT_APP_PATH, parentAppPath);
                }
                log.warn("New value for {} is {}", env, content);
                parameter.setTextContent(content);
            }

            if (actionType.equals("spark")) {
                String nodeName = parameter.getNodeName();
                if (nodeName.equals("spark-opts")) {
                    String newSparkContent = parameter.getTextContent().replace(PRODUCTION_PARENT_APP_PATH, parentAppPath);
                    log.warn("New spark-opts content {}", newSparkContent);
                    parameter.setTextContent(newSparkContent);
                }
            }
        }
    }
}
