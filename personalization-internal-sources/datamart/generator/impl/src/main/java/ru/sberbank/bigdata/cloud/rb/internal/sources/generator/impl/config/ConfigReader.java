package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;

import java.util.List;
import java.util.Optional;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.PROPERTY_SEPARATOR;

public class ConfigReader {

    public Optional<String> getPropertyFromConfig(final String systemPropertyPath, final String propertyKey) {
        List<String> content = HDFSHelper.readFile(systemPropertyPath);
        return content.stream()
                .filter(line -> line.startsWith(propertyKey))
                .map(line -> line.split(PROPERTY_SEPARATOR))
                .filter(prop -> prop.length == 2)
                .map(prop -> prop[1])
                .findFirst();
    }

    public String getTableNameFromConfig(final String systemPropertyPath) {
        return getPropertyFromConfig(systemPropertyPath, "spark.result.table")
                .orElseThrow(() -> new IllegalStateException("No spark.result.table property in config file " + systemPropertyPath));
    }
}
