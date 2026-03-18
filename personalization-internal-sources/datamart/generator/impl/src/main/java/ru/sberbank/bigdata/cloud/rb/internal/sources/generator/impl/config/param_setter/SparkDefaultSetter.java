package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config.param_setter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.util.FileUtils;

import java.io.UncheckedIOException;
import java.nio.file.Paths;

/**
 * Class find placeholder [sparkAppDefaultConf] in text
 * and replace it to default spark parameters from path PropertyConstants.SPARK_CONF_PATH
 */
public class SparkDefaultSetter extends ParamSetter {
    private final Logger log = LoggerFactory.getLogger(SparkDefaultSetter.class);

    @Override
    public String run(String property) {
        String params = defaultProperty();
        if (params.isEmpty()) {
            throw new IllegalStateException("Failed to read default spark config");
        }
        return property.replace("[sparkAppDefaultConf]", params);
    }

    @VisibleForTesting
    protected String defaultProperty() {
        try {
            return FileUtils.readAsString(Paths.get(PropertyConstants.SPARK_CONF_PATH));
        } catch (UncheckedIOException ex) {
            return StringUtils.EMPTY;
        }
    }

    @Override
    public Logger getLogger() {
        return log;
    }
}
