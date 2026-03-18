package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumVPlEpkMover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "v-pl-epk-mover-daily";
    }

    @Override
    public String getPath() {
        return "VPlEpkMover";
    }
}
