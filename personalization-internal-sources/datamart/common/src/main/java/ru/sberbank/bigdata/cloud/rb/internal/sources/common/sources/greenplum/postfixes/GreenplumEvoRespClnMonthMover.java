package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumEvoRespClnMonthMover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "evo-resp-cln-month-mover-daily";
    }

    @Override
    public String getPath() {
        return "EvoRespClnMonthMover";
    }
}