package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.smartvista.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class SmartvistaAggNonClientsPriorKafka implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "agg-nonclients-prior-kafka";
    }

    @Override
    public String getPath() {
        return "agg-nonclients-prior-kafka";
    }
}
