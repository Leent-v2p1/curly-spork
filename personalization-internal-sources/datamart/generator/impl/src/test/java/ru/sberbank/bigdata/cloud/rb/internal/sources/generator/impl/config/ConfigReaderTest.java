package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.test_api.HadoopEnv;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class ConfigReaderTest {

    @BeforeAll
    static void beforeAll() {
        HadoopEnv.setEnvVariables();
    }

    @Test
    void readProperty() {
        ConfigReader configReader = new ConfigReader();
        String value = configReader.getPropertyFromConfig("src/test/resources/config/config_reader/system.conf", "spark.result.table")
                .orElse(null);
        assertEquals("test_table", value);
    }

    @Test
    void noSuchProperty() {
        ConfigReader configReader = new ConfigReader();
        Optional<String> value = configReader.getPropertyFromConfig("src/test/resources/config/config_reader/system-no-key.conf", "spark.result.table");
        assertFalse(value.isPresent());
    }

    @Test
    void getTableName() {
        ConfigReader configReader = spy(new ConfigReader());
        doReturn(Optional.of("test_table")).when(configReader).getPropertyFromConfig(eq("path"), eq("spark.result.table"));
        String tableName = configReader.getTableNameFromConfig("path");
        assertEquals("test_table", tableName);
    }

    @Test
    void noTableName() {
        ConfigReader configReader = spy(new ConfigReader());
        doReturn(Optional.empty()).when(configReader).getPropertyFromConfig(eq("path"), eq("spark.result.table"));
        IllegalStateException path = assertThrows(IllegalStateException.class, () -> configReader.getTableNameFromConfig("path"));
        assertEquals("No spark.result.table property in config file path", path.getMessage());
    }
}