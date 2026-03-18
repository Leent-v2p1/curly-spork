package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history;

import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.ACTION;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.workflow.WORKFLOWAPP;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RecoveredReplicaWorkflowGeneratorTest {

    @Test
    void testGenerate() throws JAXBException {
        final RecoveredReplicaWorkflowGenerator recoveryGenerator =
                new RecoveredReplicaWorkflowGenerator(
                        FullTableName.of("test.parentDatamartTableName"),
                        false,
                        LocalDate.of(2018, Month.JANUARY, 31),
                        "/oozie-app/wf/custom/rb/way4/generated/create-card-wf",
                        "/oozie-app/wf/custom/rb/utils/history/create-custom-rb-card-card-history-stg-wf",
                        Environment.PRODUCTION, LocalDate.of(2018, Month.FEBRUARY, 1));
        final JAXBContext context = JAXBContext.newInstance(WORKFLOWAPP.class);
        final Unmarshaller unmarshaller = context.createUnmarshaller();
        final WORKFLOWAPP workflowapp = (WORKFLOWAPP) ((JAXBElement) unmarshaller.unmarshal(new StringReader(recoveryGenerator.generate())))
                .getValue();

        final List<Object> decisionOrForkOrJoin = workflowapp.getDecisionOrForkOrJoin();
        assertThat(decisionOrForkOrJoin, hasSize(4));
        assertAction(decisionOrForkOrJoin,
                0,
                "replica-on-the-date-2018-01-31",
                "/oozie-app/wf/custom/rb/utils/history/create-custom-rb-card-card-history-stg-wf",
                "dm-on-the-date-2018-01-31");

        assertAction(decisionOrForkOrJoin,
                1,
                "dm-on-the-date-2018-01-31",
                "/oozie-app/wf/custom/rb/way4/generated/create-card-wf",
                "dm-on-the-date-2018-02-01");

        assertAction(decisionOrForkOrJoin,
                2,
                "dm-on-the-date-2018-02-01",
                "/oozie-app/wf/custom/rb/way4/generated/create-card-wf",
                "end");
    }

    @Test
    void testGenerate_firstLoading() throws JAXBException {
        final RecoveredReplicaWorkflowGenerator recoveryGenerator =
                new RecoveredReplicaWorkflowGenerator(
                        FullTableName.of("test.parentDatamartTableName"),
                        true,
                        LocalDate.of(2018, Month.JANUARY, 31),
                        "/oozie-app/wf/custom/rb/way4/generated/create-card-wf",
                        "/oozie-app/wf/custom/rb/utils/history/create-custom-rb-card-card-history-stg-wf",
                        Environment.PRODUCTION, LocalDate.of(2018, Month.FEBRUARY, 1));
        final JAXBContext context = JAXBContext.newInstance(WORKFLOWAPP.class);
        final Unmarshaller unmarshaller = context.createUnmarshaller();
        final WORKFLOWAPP workflowapp = (WORKFLOWAPP) ((JAXBElement) unmarshaller.unmarshal(new StringReader(recoveryGenerator.generate())))
                .getValue();

        final List<Object> decisionOrForkOrJoin = workflowapp.getDecisionOrForkOrJoin();
        assertThat(decisionOrForkOrJoin, hasSize(2));

        assertAction(decisionOrForkOrJoin,
                0,
                "dm-on-the-date-2018-02-01",
                "/oozie-app/wf/custom/rb/way4/generated/create-card-wf",
                "end");
    }

    @Test
    void testGenerate_replicaIsUpToDate() throws JAXBException {
        final RecoveredReplicaWorkflowGenerator recoveryGenerator =
                new RecoveredReplicaWorkflowGenerator(
                        FullTableName.of("test.parentDatamartTableName"),
                        false,
                        LocalDate.of(2018, Month.FEBRUARY, 1),
                        "/oozie-app/wf/custom/rb/way4/generated/create-card-wf",
                        "/oozie-app/wf/custom/rb/utils/history/create-custom-rb-card-card-history-stg-wf",
                        Environment.PRODUCTION, LocalDate.of(2018, Month.FEBRUARY, 1));
        final JAXBContext context = JAXBContext.newInstance(WORKFLOWAPP.class);
        final Unmarshaller unmarshaller = context.createUnmarshaller();
        final WORKFLOWAPP workflowapp = (WORKFLOWAPP) ((JAXBElement) unmarshaller.unmarshal(new StringReader(recoveryGenerator.generate())))
                .getValue();

        final List<Object> decisionOrForkOrJoin = workflowapp.getDecisionOrForkOrJoin();
        assertThat(decisionOrForkOrJoin, hasSize(2));

        assertAction(decisionOrForkOrJoin,
                0,
                "dm-on-the-date-2018-02-01",
                "/oozie-app/wf/custom/rb/way4/generated/create-card-wf",
                "end");
    }

    @Test
    void testGenerate_EnvironmentTest() throws JAXBException {
        final RecoveredReplicaWorkflowGenerator recoveryGenerator = new RecoveredReplicaWorkflowGenerator(
                FullTableName.of("test.parentDatamartTableName"),
                false,
                LocalDate.of(2018, Month.JANUARY, 1),
                "/oozie-app/test",
                "/oozie-app/test2",
                Environment.PRODUCTION_TEST,
                LocalDate.of(2018, Month.FEBRUARY, 2)
        );

        final JAXBContext context = JAXBContext.newInstance(WORKFLOWAPP.class);
        final Unmarshaller unmarshaller = context.createUnmarshaller();
        String generate = recoveryGenerator.generate();
        System.out.println(generate);
        final WORKFLOWAPP workflowapp = (WORKFLOWAPP) ((JAXBElement) unmarshaller.unmarshal(new StringReader(generate))).getValue();
        final List<Object> actionList = workflowapp.getDecisionOrForkOrJoin();

        assertThat(actionList, hasSize(4));
        assertAction(actionList, 0, "replica-on-the-date-2018-02-01", "/oozie-app/test2", "dm-on-the-date-2018-02-01");
        assertAction(actionList, 1, "dm-on-the-date-2018-02-01", "/oozie-app/test", "dm-on-the-date-2018-02-02");
        assertAction(actionList, 2, "dm-on-the-date-2018-02-02", "/oozie-app/test", "end");
    }

    private void assertAction(List<Object> decisionOrForkOrJoin,
                              int index,
                              String expectedName,
                              String expectedAppPath,
                              String expectedOkTransition) {
        final ACTION action = (ACTION) decisionOrForkOrJoin.get(index);
        assertThat(action, notNullValue());
        assertEquals(expectedName, action.getName());
        assertEquals(expectedAppPath, action.getSubWorkflow().getAppPath());
        assertThat(action.getSubWorkflow().getPropagateConfiguration(), notNullValue());
        assertEquals(expectedOkTransition, action.getOk().getTo());
    }
}