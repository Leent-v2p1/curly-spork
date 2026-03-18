package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumBoSecurityListFullMover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dic-security-list-full-mover-daily";
    }

    @Override
    public String getPath() {
        return "BoSecurityListFullMover";
    }
}
