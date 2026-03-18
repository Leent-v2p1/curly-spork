package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service;

import org.apache.commons.io.FileUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Класс, который подставляет параметры в wfs.yaml, т.к. ctl-client этого не делает.
 * Можно удалить, когда данная функциональность появится в ctl-client.
 * Запускать можно командой: java -Dvariables-file=all.yml -Dtemplate-file=autopay-wfs.yaml -jar personalization.jar
 */
public class FillTemplate {

    public static void main(String[] args) throws IOException {
        String variablesFileName = SysPropertyTool.safeSystemProperty("variables-file");
        File variablesFile = new File(variablesFileName);
        String templateFileName = SysPropertyTool.safeSystemProperty("template-file");
        File templateFile = new File(templateFileName);
        String template = FileUtils.readFileToString(templateFile);
        YamlPropertiesParser propertiesParser = new YamlPropertiesParser();
        Map<String, Object> parse = propertiesParser.parse(variablesFile);
        for (Map.Entry<String, Object> property : parse.entrySet()) {
            String prop = "\\{\\{" + property.getKey() + "}}";
            String val = (String) property.getValue();
            template = template.replaceAll(prop, val);
        }
        FileUtils.writeStringToFile(templateFile, template);
    }
}
