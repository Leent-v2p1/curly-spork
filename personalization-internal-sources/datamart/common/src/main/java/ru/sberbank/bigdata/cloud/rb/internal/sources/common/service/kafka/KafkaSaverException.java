package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.kafka;

public class KafkaSaverException extends RuntimeException {
    public KafkaSaverException() {
        super();
    }

    public KafkaSaverException(String message) {
        super(message);
    }
}
