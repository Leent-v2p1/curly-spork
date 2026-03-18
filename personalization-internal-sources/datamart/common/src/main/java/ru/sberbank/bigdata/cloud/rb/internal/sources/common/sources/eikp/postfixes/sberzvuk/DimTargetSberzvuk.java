package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sberzvuk;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimTargetSberzvuk implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-target-sberzvuk-daily";
    }
    @Override
    public String getPath() {
        return "dim-target-sberzvuk";
    }
}