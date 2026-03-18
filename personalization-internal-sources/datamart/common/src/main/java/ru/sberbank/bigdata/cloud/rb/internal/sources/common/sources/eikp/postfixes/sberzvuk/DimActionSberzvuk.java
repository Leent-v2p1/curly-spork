package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sberzvuk;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimActionSberzvuk implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "dim-action-sberzvuk-daily";
    }
    @Override
    public String getPath() {
        return "dim-action-sberzvuk";
    }
}