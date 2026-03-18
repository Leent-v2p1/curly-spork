package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.kafka;

public enum TestTopicName {

    TOPIC_1("CLSKLROZN.DATASTREAM_EIKP_V1EVENT.V1"),
    TOPIC_2("CLSKLROZN.DATASTREAM_EIKP_V2EVENT.V1"), 
    TOPIC_3("CLSKLROZN.DATASTREAM_EIKP_V3EVENT.V1"),
    TOPIC_4("CLSKLROZN.DATASTREAM_EIKP_V4EVENT.V1");

    private String topicName;
    TestTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }
}
