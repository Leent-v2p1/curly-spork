package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.sberzvuk;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class ContactSberzvuk implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "contact-sberzvuk-daily";
    }

    @Override
    public String getPath() {
        return "contact-sberzvuk";
    }
}