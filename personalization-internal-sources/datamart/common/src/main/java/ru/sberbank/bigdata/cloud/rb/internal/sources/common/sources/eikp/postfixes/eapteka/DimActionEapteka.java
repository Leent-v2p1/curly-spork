package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.eapteka;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class DimActionEapteka implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "dim-action-eapteka-daily";
    }

    @Override
    public String getPath() {
        return "dim-action-eapteka";
    }
}
