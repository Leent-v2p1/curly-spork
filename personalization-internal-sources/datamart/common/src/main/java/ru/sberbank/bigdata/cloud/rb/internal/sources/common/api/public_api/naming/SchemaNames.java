package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;

import java.util.Arrays;
import java.util.List;

public class SchemaNames {

    public static final String userName = SysPropertyTool.getSystemProperty("spark.user.name", "user");

    public static final DbNameImpl SBOL = new DbNameImpl("custom_rb_sbol");

    /**
     * The method constructs database's name from DynamicSchemaPrefix and SourcePostfix
     *
     * @param schemaPrefix enum value from DynamicSchemaPrefix
     * @param instance dynamic instance of source
     *
     * @return schema name for given schema prefix and dynamic source postfix
     */
    public static DbNameImpl dynamicSchema(DynamicSchemaPrefix schemaPrefix, SourcePostfix instance) {
        return new DbNameImpl(schemaPrefix.getSchemaPrefix() + instance.getPostfix());
    }

    public static List<DbName> getReplicaNamesForMsckRepair() {
        return Arrays.asList(IKFL, IKFL2, IKFL3, IKFL4, IKFL5, IKFL6, IKFL7, IKFL_GF,
                IKFL_DIFF, IKFL2_DIFF, IKFL3_DIFF, IKFL4_DIFF, IKFL5_DIFF, IKFL6_DIFF, IKFL7_DIFF, IKFL_GF_DIFF,
                WAY4, SMARTVISTA_CFS);
    }

    public enum DynamicSchemaPrefix {
        ERIB_REPLICA("internal_erib_srb_");

        private final String schemaPrefix;

        DynamicSchemaPrefix(String schemaPrefix) {
            this.schemaPrefix = schemaPrefix;
        }

        public String getSchemaPrefix() {
            return schemaPrefix;
        }
    }
}
