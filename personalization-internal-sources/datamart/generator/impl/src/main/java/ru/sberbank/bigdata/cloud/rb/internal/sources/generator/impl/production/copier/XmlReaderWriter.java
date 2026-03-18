package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.copier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class XmlReaderWriter {

    private static final Logger log = LoggerFactory.getLogger(XmlReaderWriter.class);

    public Document readXml(Path filePath) {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            return documentBuilder.parse(filePath.toFile());
        } catch (SAXException | ParserConfigurationException | IOException e) {
            throw new DatamartRuntimeException(e);
        }
    }

    public void writeXmlToTargetDir(Document xml, Path filePath) {
        try {
            File file = filePath.toFile();
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(xml);
            boolean mkdirsStatus = file.getParentFile().mkdirs();
            log.debug("Create parent directory status {}" , mkdirsStatus);
            if (!file.exists()) {
                boolean newFileStatus = file.createNewFile();
                log.debug("Create file status {}" , newFileStatus);
            }
            StreamResult result = new StreamResult(file);
            transformer.transform(source, result);
        } catch (TransformerException | IOException e) {
            throw new DatamartRuntimeException(e);
        }
    }
}
