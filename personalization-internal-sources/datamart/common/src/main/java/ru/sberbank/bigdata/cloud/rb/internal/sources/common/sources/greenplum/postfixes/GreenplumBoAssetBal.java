package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.greenplum.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class GreenplumBoAssetBal implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "bo-asset-bal-daily";
    }

    @Override
    public String getPath() {
        return "BoAssetBal";
    }
}
