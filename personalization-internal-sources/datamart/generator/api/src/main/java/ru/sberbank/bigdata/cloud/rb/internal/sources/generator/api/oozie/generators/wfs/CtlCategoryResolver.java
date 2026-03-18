package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.wfs;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourceInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.WorkflowOnSupportResolver;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory.PERSONALIZATION_INTERNAL;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlCategory.PERSONALIZATION_INTERNAL_SUPPORT;

public class CtlCategoryResolver {

    private final Environment env;
    private final WorkflowOnSupportResolver workflowOnScheduleResolver;

    public CtlCategoryResolver(Environment env, WorkflowOnSupportResolver workflowOnScheduleResolver) {
        this.env = env;
        this.workflowOnScheduleResolver = workflowOnScheduleResolver;
    }

    public CtlCategory resolve(String schemaAlias, SourceInstance sourceInstance) {
        if (env.isTestEnvironment()) {
            return CtlCategory.PERSONALIZATION_INTERNAL_VERIFICATION;
        }
        return workflowOnScheduleResolver.resolve(schemaAlias, sourceInstance)
                ? PERSONALIZATION_INTERNAL_SUPPORT
                : PERSONALIZATION_INTERNAL;
    }
}
