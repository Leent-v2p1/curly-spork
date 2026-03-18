package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EribSbolConnectors implements SourcePostfix {

    @Override
    public String getPath() {
        return "sbol-connectors";
    }

    @Override
    public String getPostfix() {
        return "sbol-connectors";
    }
}