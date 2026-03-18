package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.eikp.postfixes.okko;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class ContactOkko implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "contact-okko-daily";
    }

    @Override
    public String getPath() {
        return "contact-okko";
    }
}