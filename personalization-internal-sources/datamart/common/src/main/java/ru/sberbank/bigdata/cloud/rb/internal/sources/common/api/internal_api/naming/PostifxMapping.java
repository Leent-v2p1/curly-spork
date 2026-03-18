package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes.CodDailyByInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes.EribDailyByInstance;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Class contains mapping of schemas with source_postfixes
 * Example:
 * custom_rb_erib -> ikfl1, ikfl2, ikfl3 ...
 */
public class PostifxMapping {

    public static final Map<SchemaAlias, List<SourcePostfix>> SOURCE_POSTFIXES_FOR_SOURCE = new EnumMap<>(SchemaAlias.class);

    static {
        PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE.put(SchemaAlias.CUSTOM_RB_SBOL, Arrays.asList(EribDailyByInstance.values()));
        PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE.put(SchemaAlias.CUSTOM_RB_SBOL_STG, Arrays.asList(EribDailyByInstance.values()));
        PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE.put(SchemaAlias.CUSTOM_RB_COD, Arrays.asList(CodDailyByInstance.values()));
        PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE.put(SchemaAlias.CUSTOM_RB_COD_STG, Arrays.asList(CodDailyByInstance.values()));
        PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE.put(SchemaAlias.CLSKLROZN_CUSTOM_RB_COD, Arrays.asList(CodDailyByInstance.values()));
        PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE.put(SchemaAlias.CUSTOM_RB_COD_PDB, Arrays.asList(CodDailyByInstance.values()));
        PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE.put(SchemaAlias.CUSTOM_RB_COD_PDB_STG, Arrays.asList(CodDailyByInstance.values()));
        PostifxMapping.SOURCE_POSTFIXES_FOR_SOURCE.put(SchemaAlias.CLSKLROZN_CUSTOM_RB_COD_PDB, Arrays.asList(CodDailyByInstance.values()));
    }
}
