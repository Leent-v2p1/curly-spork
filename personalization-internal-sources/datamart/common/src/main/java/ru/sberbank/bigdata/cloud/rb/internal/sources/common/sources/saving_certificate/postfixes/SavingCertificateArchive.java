package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.saving_certificate.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class SavingCertificateArchive implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "archive";
    }

    @Override
    public String getPath() {
        return "archive";
    }
}
