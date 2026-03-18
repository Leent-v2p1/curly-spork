package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.wfs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DailySourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourceInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.PrereqsReader;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites.*;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.test_api.DatamartTest;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CtlWorkflowGeneratorTest extends DatamartTest {

    public static final SourceInstance SOURCE_INSTANCE = SourceInstance
            .getSourceInstance(new DailySourcePostfix());
    private static final LocalDateTime TEST_BUILD_TIME = LocalDate.of(2000, 1, 1).atStartOfDay();

    @Test
    @DisplayName("генерирует wfs для источника c cron расписанием")
    void generateWorkflow() {
        //given
        final PrereqsReader prereqsReaderMcok = mock(PrereqsReader.class);
        final DependencyConf dependencyConf = new DependencyConf(null, null, new WorkflowParameters(), "or", new Cron("* * * * 1", true));
        when(prereqsReaderMcok.prereqConf(any())).thenReturn(dependencyConf);
        final PathBuilder pathBuilderMock = mock(PathBuilder.class);
        when(pathBuilderMock.resolveWorkflowPathForCtl(any(), any())).thenReturn("test_path/way3");
        final CtlWorkflowGenerator ctlWorkflowGenerator = new CtlWorkflowGenerator(prereqsReaderMcok, pathBuilderMock, Environment.PRODUCTION, null, null, null, TEST_BUILD_TIME);
        final String schemaAlias = "way3";
        //when
        final String generatedWorkflow = ctlWorkflowGenerator.generateWorkflow(schemaAlias, ScheduleType.START, CtlCategory.PERSONALIZATION_INTERNAL, SOURCE_INSTANCE);
        String fileContent = generatedWorkflow.replaceFirst(" {4}- ", "")
                .replace("{{common.ctl_v5.profile}}", "Default")
                .replace("{{common.ctl_v5.workflow.prefix}}", "prod");
        final Map<String, Object> workflow = new YamlPropertiesParser().parseString(fileContent);
        //then
        assertWorkflow(workflow);
    }

    @Test
    void testLocks() {
        //given
        final DependencyConf dependencyConf = createConfWithLocks();
        final PrereqsReader prereqsReaderMcok = mock(PrereqsReader.class);
        when(prereqsReaderMcok.prereqConf(any())).thenReturn(dependencyConf);
        final PathBuilder pathBuilderMock = mock(PathBuilder.class);
        when(pathBuilderMock.resolveWorkflowPathForCtl(any(), any())).thenReturn("test_path/way2");
        final CtlWorkflowGenerator ctlWorkflowGenerator = new CtlWorkflowGenerator(prereqsReaderMcok, pathBuilderMock, Environment.PRODUCTION, null, null, null, TEST_BUILD_TIME);
        final String schemaAlias = "way2";
        //when
        final String generatedWorkflow = ctlWorkflowGenerator.generateWorkflow(schemaAlias, ScheduleType.SCHEDULE, CtlCategory.PERSONALIZATION_INTERNAL, SOURCE_INSTANCE);
        System.out.println(generatedWorkflow);
        String fileContent = generatedWorkflow.replaceFirst(" {4}- ", "")
                .replace("{{common.ctl_v5.profile}}", "Default")
                .replace("{{common.ctl_v5.workflow.prefix}}", "prod")
                .replace("{{common.ctl_v5.source_stat_profile}}", "replica_profile");
        final Map<String, Object> workflow = new YamlPropertiesParser().parseString(fileContent);
        //then
        assertLocks(workflow);
    }

    private void assertLocks(Map<String, Object> workflow) {
        final String expectedWfName = "masspers-prod-way2-daily-datamarts-wf";
        @SuppressWarnings("unchecked") final List<Map<String, Map<String, Object>>> checks =
                (List<Map<String, Map<String, Object>>>) workflow.get(expectedWfName + ".init_locks.checks");
        assertThat(checks, hasSize(1));
        final Map<String, Object> check = checks.get(0).get("check");
        assertEquals(1, check.get("entity_id"));
        assertEquals("WRITE", check.get("lock"));
        assertEquals("INIT", check.get("lock_group"));
        assertEquals("my_profile", check.get("profile"));

        @SuppressWarnings("unchecked") final List<Map<String, Map<String, Object>>> sets =
                (List<Map<String, Map<String, Object>>>) workflow.get(expectedWfName + ".init_locks.sets");
        assertThat(sets, hasSize(2));
        final Map<String, Object> set1 = sets.get(0).get("set");
        assertEquals(2, set1.get("entity_id"));
        assertEquals("WRITE", set1.get("lock"));
        assertEquals("INIT", set1.get("lock_group"));
        assertEquals(20, set1.get("estimate"));
        assertEquals("my_profile", set1.get("profile"));
        final Map<String, Object> set2 = sets.get(1).get("set");
        assertEquals(3, set2.get("entity_id"));
        assertEquals("READ", set2.get("lock"));
        assertEquals("INIT", set2.get("lock_group"));
        assertEquals(20, set2.get("estimate"));
        assertEquals("Default", set2.get("profile"));
    }

    private DependencyConf createConfWithLocks() {
        final DependencyConf dependencyConf = new DependencyConf(null, null, new WorkflowParameters(), "or", null);
        final Locks locks = new Locks();
        locks.setChecks(new LockCheck[]{new LockCheck("1", "WRITE", "my_profile", "INIT")});
        locks.setSets(new LockSet[]{
                new LockSet("2", "WRITE", "my_profile", "20", "INIT"),
                new LockSet("3", "READ", null, "20", "INIT")
        });
        dependencyConf.setLocks(locks);
        return dependencyConf;
    }

    @Test
    @DisplayName("генерирует wfs для источника c зависимостями от entity")
    void generateWorkflowWithEntitiesDependency() {
        //given
        final PrereqsReader prereqsReaderMcok = mock(PrereqsReader.class);
        final DependencyConf dependencyConf = createDependencyConf("and");
        when(prereqsReaderMcok.prereqConf(any())).thenReturn(dependencyConf);
        final PathBuilder pathBuilderMock = mock(PathBuilder.class);
        when(pathBuilderMock.resolveWorkflowPathForCtl(any(), any())).thenReturn("test_path/way2");
        final CtlWorkflowGenerator ctlWorkflowGenerator = new CtlWorkflowGenerator(prereqsReaderMcok, pathBuilderMock, Environment.PRODUCTION, null, null,null, TEST_BUILD_TIME);
        final String schemaAlias = "way2";
        //when
        final String generatedWorkflow = ctlWorkflowGenerator.generateWorkflow(schemaAlias, ScheduleType.SCHEDULE, CtlCategory.PERSONALIZATION_INTERNAL, SOURCE_INSTANCE);
        String fileContent = generatedWorkflow.replaceFirst(" {4}- ", "")
                .replace("{{common.ctl_v5.profile}}", "Default")
                .replace("{{common.ctl_v5.workflow.prefix}}", "prod")
                .replace("{{common.ctl_v5.source_stat_profile}}", "replica_profile");
        final Map<String, Object> workflow = new YamlPropertiesParser().parseString(fileContent);
        //then
        assertWorkflowWithDependency(workflow);
    }

    @Test
    @DisplayName("генерирует wfs для источника, который разбит на под-источники (ERIB, COD)")
    void generateWorkflowWitSourceModifier() {
        //given
        final PrereqsReader prereqsReaderMcok = mock(PrereqsReader.class);
        final DependencyConf dependencyConf = createDependencyConf("and");
        when(prereqsReaderMcok.prereqConf(any())).thenReturn(dependencyConf);
        final PathBuilder pathBuilderMock = mock(PathBuilder.class);
        when(pathBuilderMock.resolveWorkflowPathForCtl(any(), any())).thenReturn("test_path/way1");
        final CtlWorkflowGenerator ctlWorkflowGenerator = new CtlWorkflowGenerator(prereqsReaderMcok, pathBuilderMock, Environment.PRODUCTION, null, null, null, TEST_BUILD_TIME);
        SourceInstance sourceInstance = new SourceInstance("ctlName", "pathName", "ikfl", "ikfl");
        final String schemaAlias = "way1";
        //when
        final String generatedWorkflow = ctlWorkflowGenerator.generateWorkflow(schemaAlias, ScheduleType.SCHEDULE, CtlCategory.PERSONALIZATION_INTERNAL, sourceInstance);
        String fileContent = generatedWorkflow.replaceFirst(" {4}- ", "")
                .replace("{{common.ctl_v5.profile}}", "Default")
                .replace("{{common.ctl_v5.workflow.prefix}}", "prod")
                .replace("{{common.ctl_v5.source_stat_profile}}", "replica_profile");
        final Map<String, Object> workflow = new YamlPropertiesParser().parseString(fileContent);
        //then
        assertWorkflowWithSourceModifier(workflow);
    }

    @Test
    void generateHeader() {
        final CtlWorkflowGenerator ctlWorkflowGenerator = new CtlWorkflowGenerator(null, null, Environment.PRODUCTION, "path/properties", "1.1.1","false", TEST_BUILD_TIME);
        final String generatedHeader = ctlWorkflowGenerator.generateHeader("erib")
                .replace("workflows:", "")
                .replace("{{security.erib.username}}", "username")
                .replace("{{common.yarn.default.queue}}", "default_queue");
        final Map<String, Object> header = new YamlPropertiesParser().parseString(generatedHeader);
        assertHeader(header);
    }

    private void assertWorkflow(Map<String, Object> wf) {
        String expectedName = "masspers-prod-way3-daily-datamarts-wf";

        assertEquals("masspers-prod-way3-daily-datamarts", wf.get(expectedName + ".name"));
        assertEquals("DHP2 Personalization internal", wf.get(expectedName + ".category"));
        assertEquals("principal", wf.get(expectedName + ".type"));
        assertEquals("oozie", wf.get(expectedName + ".orchestrator"));
        assertEquals("or", wf.get(expectedName + ".schedule_params.eventAwaitStrategy"));
        assertEquals("* * * * 1", wf.get(expectedName + ".schedule_params.cron.expression"));
        assertEquals(true, wf.get(expectedName + ".schedule_params.cron.active"));
        assertEquals("start", wf.get(expectedName + ".schedule.type"));
        assertEquals("Default", wf.get(expectedName + ".profile"));
        assertEquals(true, wf.get(expectedName + ".singleLoading"));

        @SuppressWarnings("unchecked")
        List<Map<String, Map<String, Object>>> wfParams = (List<Map<String, Map<String, Object>>>) wf.get(expectedName + ".params");
        assertThat(wfParams, hasItem(paramMap("distribution-build-time", "distribution.build.time", "2000-01-01T00:00")));
        assertThat(wfParams, hasItem(paramMap("distribution-build-version", "distribution.build.version", "{{common.build.version}}")));
        assertThat(wfParams, hasItem(paramMap("application-name", "appName", "masspers-prod-way3-daily-datamarts")));
        assertThat(wfParams, hasItem(paramMap("application-path", "oozie.wf.application.path", "test_path/way3")));
        assertThat(wfParams, hasItem(paramMap("oozie-use-system-libpath", "oozie.use.system.libpath", true)));
        assertThat(wfParams, hasItem(paramMap("skip-if-built-today", "skipIfBuiltToday", false)));
    }

    private void assertWorkflowWithDependency(Map<String, Object> wf) {
        String expectedName = "masspers-prod-way2-daily-datamarts-wf";

        assertEquals("masspers-prod-way2-daily-datamarts", wf.get(expectedName + ".name"));
        assertEquals("DHP2 Personalization internal", wf.get(expectedName + ".category"));
        assertEquals("principal", wf.get(expectedName + ".type"));
        assertEquals("oozie", wf.get(expectedName + ".orchestrator"));
        assertEquals("and", wf.get(expectedName + ".schedule_params.eventAwaitStrategy"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> entities = (List<Map<String, Object>>) wf.get(expectedName + ".schedule_params.entities");
        final List<Map<String, Map<String, Object>>> expectedEntities = asList(
                entityMap(1, "clsklrozn"),
                entityMap(2, "replica_profile"),
                entityMap(3, "Default"),
                entityMap(4, "Default")
        );
        assertEquals(entities, expectedEntities);
        assertEquals("on-schedule", wf.get(expectedName + ".schedule.type"));
        assertEquals("Default", wf.get(expectedName + ".profile"));

        @SuppressWarnings("unchecked")
        List<Map<String, Map<String, Object>>> wfParams = (List<Map<String, Map<String, Object>>>) wf.get(expectedName + ".params");
        assertThat(wfParams, hasItem(paramMap("distribution-build-time", "distribution.build.time", "2000-01-01T00:00")));
        assertThat(wfParams, hasItem(paramMap("distribution-build-version", "distribution.build.version", "{{common.build.version}}")));
        assertThat(wfParams, hasItem(paramMap("application-name", "appName", "masspers-prod-way2-daily-datamarts")));
        assertThat(wfParams, hasItem(paramMap("application-path", "oozie.wf.application.path", "test_path/way2")));
        assertThat(wfParams, hasItem(paramMap("oozie-use-system-libpath", "oozie.use.system.libpath", true)));
    }

    private void assertWorkflowWithSourceModifier(Map<String, Object> wf) {
        String expectedName = "masspers-prod-way1-ikfl-datamarts-wf";

        assertEquals("masspers-prod-way1-ikfl-datamarts", wf.get(expectedName + ".name"));
        @SuppressWarnings("unchecked")
        List<Map<String, Map<String, Object>>> wfParams = (List<Map<String, Map<String, Object>>>) wf.get(expectedName + ".params");
        assertThat(wfParams, hasItem(paramMap("distribution-build-time", "distribution.build.time", "2000-01-01T00:00")));
        assertThat(wfParams, hasItem(paramMap("distribution-build-version", "distribution.build.version", "{{common.build.version}}")));
        assertThat(wfParams, hasItem(paramMap("application-name", "appName", "masspers-prod-way1-ikfl-datamarts")));
        assertThat(wfParams, hasItem(paramMap("source-instance", "ctlName", "ikfl")));
    }

    private void assertHeader(Map<String, Object> header) {
        assertEquals("default_queue", header.get("global.yarn-queue"));
        assertEquals("production", header.get("global.environment"));
        assertEquals("path/properties", header.get("global.properties-path"));
        assertEquals("1.1.1", header.get("global.ctl-version-required"));
        assertEquals("{{common.metric.graphite.carbon.host}}", header.get("global.graphite.carbon.host"));
        assertEquals("{{common.metric.graphite.carbon.port}}", header.get("global.graphite.carbon.port"));
        assertEquals("{{common.ctl_v5.profile}}", header.get("global.profile"));
        assertEquals("{{common.ctl_v5.workflow.prefix}}", header.get("global.wf_prefix"));
        assertEquals("username", header.get("global.user.name"));
    }

    private Map<String, Map<String, Object>> paramMap(String key, String name, Object value) {
        return mapOfMaps(key, new Tuple2<>("name", name), new Tuple2<>("prior_value", value));
    }

    private Map<String, Map<String, Object>> entityMap(Integer id, String profile) {
        return mapOfMaps("entity", new Tuple2<>("id", id), new Tuple2<>("profile", profile), new Tuple2<>("statisticId", 11), new Tuple2<>("active", true));
    }

    @SafeVarargs
    private final Map<String, Map<String, Object>> mapOfMaps(String name, Tuple2<String, Object>... entries) {
        Map<String, Object> innerMap = Arrays.stream(entries).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        return singletonMap(name, innerMap);
    }

    private DependencyConf createDependencyConf(String awaitStrategy) {
        CommonDependency[] replicaDependency = {
                new CommonDependency(11, "clsklrozn", new Integer[]{1}),
                new CommonDependency(11, "{{common.ctl_v5.source_stat_profile}}", new Integer[]{2})
        };
        CommonDependency[] datamartDependency = {new CommonDependency(11, "{{common.ctl_v5.profile}}", new Integer[]{3, 4})};
        return new DependencyConf(replicaDependency, datamartDependency, new WorkflowParameters(), awaitStrategy, null);
    }
}