package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.ActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.ShellActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.builders.actions.SubWfActionParamsBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConfReader;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ActionTypeMapperTest {

    @Test
    void resolve() {
        //given:
        final ActionConf subWfactionConf = new ActionConf();
        subWfactionConf.setName("action_1");
        subWfactionConf.setType("sub-wf");
        final ActionConf datamartActionConf = new ActionConf();
        datamartActionConf.setName("action_2");
        datamartActionConf.setType("datamart");
        final ActionConf[] confs = {subWfactionConf, datamartActionConf};
        List<ActionConf> actionConfList = Arrays.asList(confs);
        Map<String, ActionConf> actionConfMap = new ActionConfReader().asMap(actionConfList);
        final ActionTypeMapperImpl actionTypeMapper = new ActionTypeMapperImpl(actionConfMap, null/*ignore*/);

        //when:
        ActionParamsBuilder subWfBuilder = actionTypeMapper.resolve("action_1");
        ActionParamsBuilder datamartWfBuilder = actionTypeMapper.resolve("action_2");
        //then:
        assertTrue(subWfBuilder instanceof SubWfActionParamsBuilder);
        assertTrue(datamartWfBuilder instanceof ShellActionParamsBuilder);
    }
}