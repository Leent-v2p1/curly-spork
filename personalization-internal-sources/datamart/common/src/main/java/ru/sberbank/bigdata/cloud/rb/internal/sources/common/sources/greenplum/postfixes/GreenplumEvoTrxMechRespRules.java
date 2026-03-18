package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEvoTrxMechRespRules implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "evo-trx-mech-resp-rules-daily";
    }

    @Override
    public String getPath() {
        return "EvoTrxMechRespRules";
    }
}