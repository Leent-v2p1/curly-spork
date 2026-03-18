package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumRepCommFunnelNaviMover implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "rep-comm-funnel-navi-mover-daily";
    }

    @Override
    public String getPath() {
        return "RepCommFunnelNaviMover";
    }
}