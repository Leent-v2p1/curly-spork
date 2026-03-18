package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class SbolLogonLauncher implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "sbol-logon-launcher";
    }

    @Override
    public String getPath() {
        return "sbol-logon-launcher";
    }
}
