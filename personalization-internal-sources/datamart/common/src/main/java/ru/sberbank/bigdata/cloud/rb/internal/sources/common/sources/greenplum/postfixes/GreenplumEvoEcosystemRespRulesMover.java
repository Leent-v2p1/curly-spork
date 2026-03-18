package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEvoEcosystemRespRulesMover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "evo-ecosystem-resp-rules-mover-daily";
    }

    @Override
    public String getPath() {
        return "EvoEcosystemRespRulesMover";
    }
}