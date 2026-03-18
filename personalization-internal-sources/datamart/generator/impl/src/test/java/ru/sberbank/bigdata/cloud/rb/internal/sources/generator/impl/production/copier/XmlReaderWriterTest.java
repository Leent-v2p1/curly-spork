package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.copier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XmlReaderWriterTest {

    private Path testFile;

    @BeforeEach
    void setUp() throws Exception {
        this.testFile = Files.createTempFile("xmlIOtest", ".xml");
    }

    @AfterEach
    void tearDown() throws Exception {
        Files.deleteIfExists(testFile);
    }

    @Test
    void readXml() {
        XmlReaderWriter xmlReaderWriter = new XmlReaderWriter();
        Path testXmlPath = Paths.get(System.getProperty("user.dir"), "/src/test/resources/generators/path_replacer/test.xml");
        Document actualXml = xmlReaderWriter.readXml(testXmlPath);
        assertNotNull(actualXml);
        NodeList xmlChildNodes = actualXml.getChildNodes();
        assertEquals(1, xmlChildNodes.getLength(), "xml should have 1 node ");
        assertEquals("workflow-app", xmlChildNodes.item(0).getNodeName(), "xml should starts with workflow-app");
    }

    @Test
    void writeXmlToTargetDir() throws IOException, ParserConfigurationException {
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        Document document = builderFactory.newDocumentBuilder().newDocument();
        Element root = document.createElement("test");
        root.setTextContent("testValue");
        document.appendChild(root);

        XmlReaderWriter xmlReaderWriter1 = new XmlReaderWriter();
        xmlReaderWriter1.writeXmlToTargetDir(document, testFile);

        List<String> strings = Files.readAllLines(testFile);
        String firstRow = strings.get(0);
        assertTrue(firstRow.contains("<test>testValue</test>"));
    }
}