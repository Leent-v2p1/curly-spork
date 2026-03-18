package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

/**
 * Класс проверяет необходимость механизме передачи данных в Kafka
 * Флагом является значение ctl параметра (или параметра в system.conf)
 * spark.datamart.kafkaSaver = true
 */
public class KafkaSaverRequiredChecker {

    private final boolean kafkaSaverRequired;

    public KafkaSaverRequiredChecker(boolean kafkaSaverRequired) {
        this.kafkaSaverRequired = kafkaSaverRequired;
    }

    public boolean isKafkaSaverRequired() {
        return kafkaSaverRequired;
    }
}
