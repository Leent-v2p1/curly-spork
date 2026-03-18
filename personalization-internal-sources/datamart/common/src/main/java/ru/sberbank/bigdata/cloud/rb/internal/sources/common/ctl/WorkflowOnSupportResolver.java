package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DailySourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourceInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class WorkflowOnSupportResolver {

    private static final Map<WorkflowKey, CtlCategory> SCHEMA_INSTANCE_MAP = new HashMap<>();

    static {
        SCHEMA_INSTANCE_MAP.put(new WorkflowKey(SchemaAlias.CUSTOM_RB_TRIGGERS, SourceInstance.getSourceInstance(new DailySourcePostfix())), CtlCategory.PERSONALIZATION_INTERNAL_SUPPORT);
        SCHEMA_INSTANCE_MAP.put(new WorkflowKey(SchemaAlias.CUSTOM_RISK_TRIG_INPUT, SourceInstance.getSourceInstance(new DailySourcePostfix())), CtlCategory.PERSONALIZATION_INTERNAL_SUPPORT);
    }

    public boolean resolve(String schemaAlias, SourceInstance sourceInstance) {
        final SchemaAlias schema = SchemaAlias.ofAlias(schemaAlias)
                .orElseThrow(() -> new IllegalStateException("No such schema alias " + schemaAlias));
        return SCHEMA_INSTANCE_MAP.containsKey(new WorkflowKey(schema, sourceInstance));
    }

    static class WorkflowKey {
        private final SchemaAlias schemaAlias;
        private final SourceInstance sourceInstance;

        WorkflowKey(SchemaAlias schemaAlias, SourceInstance sourceInstance) {
            this.schemaAlias = schemaAlias;
            this.sourceInstance = sourceInstance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WorkflowKey that = (WorkflowKey) o;
            return schemaAlias == that.schemaAlias &&
                    Objects.equals(sourceInstance, that.sourceInstance);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaAlias, sourceInstance);
        }
    }
}
