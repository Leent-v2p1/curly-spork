package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.LoadingWithStatusResponse;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.Workflow;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.WorkflowParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class WfParamListTest {

    @SneakyThrows
    @Test
    void testGetParams() {
        final Integer loadingId = 123;
        CtlApiCalls apiCalls = mock(CtlApiCallsV1.class);
        Workflow workflow = new Workflow();
        workflow.setParam(new ArrayList<>());
        final String PARAM1 = "param1";
        final String VAL1 = "val1";
        final String PARAM2 = "param2";
        final String VAL2 = "val2";
        workflow.getParam().add(new WorkflowParam(1, PARAM1, VAL1));
        workflow.getParam().add(new WorkflowParam(2, PARAM2, VAL2));
        LoadingWithStatusResponse loadingWithStatusResponse = new LoadingWithStatusResponse(
                false, workflow, null, null, null, null
                , 1, null, null, 1, null, null);
        doReturn(loadingWithStatusResponse).when(apiCalls).getLoading(loadingId);
        Map<String, String> result = new WfParam(apiCalls, 123).get();
        Map<String, String> expected = new HashMap<>();
        expected.put(PARAM1, VAL1);
        expected.put(PARAM2, VAL2);
        assertThat(result.entrySet(), equalTo(expected.entrySet()));
    }

    @Test
    void createFromSystemProperties() {
        System.setProperty("spark.ctl.url", "http://url");
        System.setProperty("spark.ctl.loading.id", "333");
        System.setProperty("spark.ctl.api.version.v5", "false");
        WfParam fromSystemProperties = WfParam.createFromSystemProperties();
        assertNotNull(fromSystemProperties);
    }
}
