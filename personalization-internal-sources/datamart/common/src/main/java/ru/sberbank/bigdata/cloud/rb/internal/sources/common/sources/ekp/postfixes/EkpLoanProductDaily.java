package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.ekp.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class EkpLoanProductDaily implements SourcePostfix {
    @Override
    public String getPostfix() {
        return "loan-product-daily";
    }

    @Override
    public String getPath() {
        return "loan-product-daily";
    }
}