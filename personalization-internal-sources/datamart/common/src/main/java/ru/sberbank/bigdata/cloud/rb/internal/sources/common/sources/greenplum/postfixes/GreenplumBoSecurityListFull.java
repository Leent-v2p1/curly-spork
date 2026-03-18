package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumBoSecurityListFull implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dic-security-list-full-daily";
    }

    @Override
    public String getPath() {
        return "BoSecurityListFull";
    }
}
