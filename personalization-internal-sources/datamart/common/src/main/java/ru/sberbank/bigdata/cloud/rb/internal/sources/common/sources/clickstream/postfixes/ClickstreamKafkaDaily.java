package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.clickstream.postfixes;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.SourcePostfix;

public class ClickstreamKafkaDaily implements SourcePostfix {

    @Override
    public String getPostfix() {
        return "kafka-daily";
    }

    @Override
    public String getPath() {
        return "kafka-daily";
    }
}
