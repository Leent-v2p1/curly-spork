package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumBoAgrmntAttrMover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "bo-agrmnt-attr-mover-daily";
    }

    @Override
    public String getPath() {
        return "BoAgrmntAttrMover";
    }
}
