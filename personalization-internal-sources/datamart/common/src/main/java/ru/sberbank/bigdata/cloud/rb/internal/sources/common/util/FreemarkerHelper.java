package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Map;

public class FreemarkerHelper {

    private static Configuration cfg;

    public static Template getTemplate(String templateName) {
        initConfig();
        try {
            return cfg.getTemplate(templateName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String printTemplate(Template template, Map<String, Object> properties) {
        StringWriter writer = new StringWriter();
        try {
            template.process(properties, writer);
        } catch (IOException | TemplateException e) {
            throw new DatamartRuntimeException(e);
        }
        return writer.toString();
    }

    private static void initConfig() {
        cfg = new Configuration(Configuration.VERSION_2_3_20);
        cfg.setClassForTemplateLoading(FreemarkerHelper.class, "/templates");
    }
}
