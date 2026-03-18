package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class AggrSalary implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "aggr-salary";
    }

    @Override
    public String getPath() {
        return "aggrSalary";
    }
}
