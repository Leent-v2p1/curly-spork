package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.config;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.WfParam;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class ColParametersTest {

    @SneakyThrows
    @Test
    void buildCtlParam() {
        Map<String, String> parametersFromCtl = new HashMap<>();
        parametersFromCtl.put("key_from_ctl", "value_from_ctl");
        WfParam ctlMock = mock(WfParam.class);
        doReturn(parametersFromCtl).when(ctlMock).get();
        System.setProperty("spark.ctl.url", "value1");
        System.setProperty("spark.ctl.loading.id", "value2");
        System.setProperty("spark.ctl.wf.id", "value3");
        System.setProperty("spark.ctl.loading.start", "value4");
        System.setProperty("spark.custom.build.date", "value5");
        CtlParameters ctlParametes = new CtlParameters(ctlMock);
        Map<String, String> resultConfig = ctlParametes.buildCtlParam();

        assertThat(resultConfig.entrySet(), hasSize(6));
        assertEquals("value1", resultConfig.get("ctl.url"));
        assertEquals("value2", resultConfig.get("ctl.loading.id"));
        assertEquals("value3", resultConfig.get("ctl.wf.id"));
        assertEquals("value4", resultConfig.get("ctl.loading.start"));
        assertEquals("value5", resultConfig.get("custom.build.date"));
        assertEquals("value_from_ctl", resultConfig.get("key_from_ctl"));
    }
}