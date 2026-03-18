package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.copier;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.PARENT_APP_PATH;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.TEST_BASE_DIR;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.TEST_UTILS_DIR;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.TEST_WORKFLOWS;

class SubWfTransformerTest {

    @Test
    void multipleAppPaths() {
        Path path = Paths.get(System.getProperty("user.dir"), "/src/test/resources/generators/path_replacer/test.xml");
        Document document = new XmlReaderWriter().readXml(path);
        SubWfTransformer subWfTransformer = new SubWfTransformer(TEST_WORKFLOWS, TEST_UTILS_DIR, PARENT_APP_PATH + TEST_BASE_DIR, Environment.PRODUCTION_TEST);

        subWfTransformer.replacePaths(document);

        NodeList appPaths = document.getElementsByTagName("app-path");
        List<String> expected = Arrays.asList(
                "/oozie-app/wf/custom/rb/production_test/utils/common/validate-ctl-version",
                "/oozie-app/wf/custom/rb/production_test/generated/frod/create-client-events-wf",
                "/oozie-app/wf/custom/rb/production_test/generated/frod/create-client-events-wf2");
        isTransformedXmlContains(appPaths, expected);

        NodeList files = document.getElementsByTagName("file");
        String expectedFile = "${nameNode}/oozie-app/wf/custom/rb/production_test/utils/common/jars/personalization-java.jar#personalization.jar";
        isTransformedXmlContains(files, Collections.singletonList(expectedFile));

        NodeList jars = document.getElementsByTagName("jar");
        String expectedJar = "${nameNode}/oozie-app/wf/custom/rb/production_test/utils/common/jars/personalization-spark.jar";
        isTransformedXmlContains(jars, Collections.singletonList(expectedJar));

        NodeList sparkOpts = document.getElementsByTagName("spark-opts");
        String sparkOptsRow = "--files ${nameNode}/oozie-app/wf/custom/rb/production_test/utils/common/custom-log4j.properties";
        assertTrue(sparkOpts.item(0).getTextContent().contains(sparkOptsRow));
    }

    private void isTransformedXmlContains(NodeList nodes, List<String> expected) {
        for (int i = 0; i < nodes.getLength(); i++) {
            String actualContent = nodes.item(i).getTextContent();
            boolean contains = expected.contains(actualContent);
            if (!contains) {
                fail("there's no " + actualContent + " in \n " + expected);
            }
        }
    }

    @Test
    void replaceWfAppName() {
        Path path = Paths.get(System.getProperty("user.dir"), "/src/test/resources/generators/path_replacer/name.xml");
        Document document = new XmlReaderWriter().readXml(path);
        new SubWfTransformer(null, null, null, Environment.PRODUCTION_TEST).replaceWfAppName(document);
        //assert name
        final Node workflowAppTag = document.getElementsByTagName("workflow-app").item(0);
        final String wfAppName = workflowAppTag.getAttributes().getNamedItem("name").getTextContent();
        assertEquals("test-some-name", wfAppName);
    }
}