package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.wfs;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourceInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.BaseProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.PropertiesUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.WorkflowOnSupportResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.PrereqsReader;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.wfs.CtlCategoryResolver;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment.PRODUCTION;

/**
 * Generates wfs.yaml config-files for release environment
 */
public class WfsGenerator {
    private final Environment env;
    private Function<String, DatamartProperties> propertiesServiceBuilder;
    private Set<String> datamartIds;
    private List<String> onSchedule;
    private CtlWorkflowGenerator ctlWorkflowGenerator;
    private CtlCategoryResolver ctlCategoryResolver;

    public WfsGenerator(Environment env) {
        this.env = env;
        initPrams();
    }

    public WfsGenerator(Function<String, DatamartProperties> propertiesServiceBuilder,
                        Set<String> datamartIds,
                        List<String> onSchedule,
                        Environment env,
                        CtlWorkflowGenerator ctlWorkflowGenerator,
                        CtlCategoryResolver ctlCategoryResolver) {
        this.propertiesServiceBuilder = propertiesServiceBuilder;
        this.datamartIds = datamartIds;
        this.onSchedule = onSchedule;
        this.env = env;
        this.ctlWorkflowGenerator = ctlWorkflowGenerator;
        this.ctlCategoryResolver = ctlCategoryResolver;
    }

    @SuppressWarnings("unchecked")
    private void initPrams() {
        BaseProperties properties = new BaseProperties();
        final String propertiesPath = properties.getPropertiesPath(env);
        final String ctlVersionRequired = properties.getCtlVersion(env);
        final String ctlApiVersionV5 = properties.getCtlApiVersionV5(env);
        final PrereqsReader prereqsReader = new PrereqsReader();

        this.propertiesServiceBuilder = datamartId -> new DatamartProperties(FullTableName.of(datamartId));
        this.datamartIds = PropertiesUtils.getAllActionsIdWithType(WorkflowType.DATAMART);
        this.onSchedule = Optional.ofNullable(properties.getOnSchedule(env)).orElse(Collections.emptyList());
        this.ctlWorkflowGenerator = new CtlWorkflowGenerator(prereqsReader, new PathBuilder(env), env, propertiesPath,
                ctlVersionRequired, ctlApiVersionV5, LocalDateTime.now());
        this.ctlCategoryResolver = new CtlCategoryResolver(env, new WorkflowOnSupportResolver());
    }

    public Map<String, String> generate() {
        final Map<String, List<DatamartProperties>> groupedBySchema = datamartIds
                .stream()
                .map(datamartId -> propertiesServiceBuilder.apply(datamartId))
                .collect(groupingBy(properties -> getAlias(properties.getTargetSchema())));

        return groupedBySchema.keySet()
                .stream()
                .map(this::generateWfsYaml)
                .collect(toMap(Tuple2::_1, Tuple2::_2));
    }

    @VisibleForTesting
    String getAlias(String schema) {
        return SchemaAlias.of(schema).schemaAliasValue;
    }

    private String generateInstanceWfs(List<SourceInstance> sourceInstances, String schemaAlias, ScheduleType scheduleType) {
        return sourceInstances
                .stream()
                .map(sourceModifier -> {
                    final CtlCategory ctlCategory = ctlCategoryResolver.resolve(schemaAlias, sourceModifier);
                    return ctlWorkflowGenerator.generateWorkflow(schemaAlias, scheduleType, ctlCategory, sourceModifier);
                })
                .collect(Collectors.joining());
    }

    private Tuple2<String, String> generateWfsYaml(String schemaAlias) {
        final ScheduleType scheduleType = getScheduleType(schemaAlias);

        final String header = ctlWorkflowGenerator.generateHeader(schemaAlias);

        final List<SourceInstance> sourceInstances = SourceInstance.resolveInstancesForSchema(schemaAlias);
        final String instanceWfs = generateInstanceWfs(sourceInstances, schemaAlias, scheduleType);

        return new Tuple2<>(schemaAlias, header + instanceWfs);
    }

    private ScheduleType getScheduleType(String schemaAlias) {
        return databaseRequiredToBeOnSchedule(schemaAlias) && env.equals(PRODUCTION)
                ? ScheduleType.SCHEDULE
                : ScheduleType.NONE;
    }

    private Boolean databaseRequiredToBeOnSchedule(String schemaAlias) {
        return this.onSchedule.contains(schemaAlias);
    }
}
