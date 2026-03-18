package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.wfs;

import freemarker.template.Template;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.EnvironmentPathResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourceInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.PrereqsReader;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.prereqs.entites.DependencyConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.FreemarkerHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.AppPath;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.WorkflowNameEscaper;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getCtlWorkflowKeytabHolder;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getCtlWorkflowName;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getCtlWorkflowUserHolder;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getCtlWorkflowQueue;

/**
 * Класс для генерации отдельных потоков ctl в wfs.yaml
 */
public class CtlWorkflowGenerator {

    private static final Template WORKFLOW_TEMPLATE = FreemarkerHelper.getTemplate("production-wfs-yaml-workflow.ftl");
    private static final Template HEADER_TEMPLATE = FreemarkerHelper.getTemplate("wfs-yaml-header.ftl");

    private final PrereqsReader prereqsReader;
    private final PathBuilder pathBuilder;
    private final Environment environment;
    private final String propertiesPath;
    private final String ctlVersionRequired;
    private final String ctlApiVersionV5;
    private final LocalDateTime buildTime;

    public CtlWorkflowGenerator(PrereqsReader prereqsReader,
                                PathBuilder pathBuilder,
                                Environment environment,
                                String propertiesPath,
                                String ctlVersionRequired,
                                String ctlApiVersionV5,
                                LocalDateTime buildTime) {
        this.prereqsReader = prereqsReader;
        this.environment = environment;
        this.pathBuilder = pathBuilder;
        this.propertiesPath = propertiesPath;
        this.ctlVersionRequired = ctlVersionRequired;
        this.ctlApiVersionV5 = ctlApiVersionV5;
        this.buildTime = buildTime;
    }

    public String generateWorkflow(String schemaAlias,
                                   ScheduleType scheduleType,
                                   CtlCategory ctlCategory,
                                   SourceInstance sourceModifier) {

        final String sourceModifierResolved = sourceModifier.getCtlWorkflowName();
        final String ctlWorkflowName = getCtlWorkflowName(environment, schemaAlias, sourceModifierResolved);

        final String dependencyConfName = schemaAlias + "-" + sourceModifierResolved;

        final String appPath = pathBuilder.resolveWorkflowPathForCtl(schemaAlias, sourceModifier);
        final DependencyConf prereqConf = prereqsReader.prereqConf(dependencyConfName);
        final String escapedValue = WorkflowNameEscaper.escape(sourceModifier.getValue());

        final Map<String, Object> params = new HashMap<>();
        params.put("distributionBuildTime", buildTime);
        params.put("ctlWorkflowName", ctlWorkflowName);
        params.put("scheduleType", scheduleType.ctlClientString);
        params.put("appPath", appPath);
        params.put("dependencyConf", prereqConf);
        params.put("category", ctlCategory.value());
        params.put("sourceInstance", new SourceInstance(sourceModifier.getCtlParamName(), sourceModifier.getWorkflowPathName(), ctlWorkflowName, escapedValue));

        return FreemarkerHelper.printTemplate(WORKFLOW_TEMPLATE, params);
    }

    public String generateHeader(String schemaAlias) {
        final String username = getCtlWorkflowUserHolder(schemaAlias);
        final String keytabPath = getCtlWorkflowKeytabHolder(schemaAlias);
        final String queue = getCtlWorkflowQueue(schemaAlias);
        final Map<String, Object> params = new HashMap<>();
        params.put("propertiesPath", propertiesPath);
        params.put("ctlVersionRequired", ctlVersionRequired);
        params.put("ctlApiVersionV5", ctlApiVersionV5);
        params.put("environment", environment.nameLowerCase());
        params.put("utilsDir", AppPath.unixPath(new EnvironmentPathResolver(environment).getUtilsDir()));
        params.put("username", username);
        params.put("keytabPath", keytabPath);
        params.put("queue", queue);
        return FreemarkerHelper.printTemplate(HEADER_TEMPLATE, params);
    }
}
