package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.kafka;

public class KafkaBigDataParams {
    private final Boolean kafkaBigDataFlag; //флаг обработки инкремента частями
    private final Integer maxInc; //максимальный инкремент передаваем в кафку за раз
    private final Integer intervalInc; //интервал между загрузками (по умолчанию 4 часа)
    private final Integer endIncPartid; //верхняя граница part_id для загрузки по умолчанию 6
    private final Integer startIncPartid; //нижняя граница part_id для загрузки по умолчанию 0

    KafkaBigDataParams(Boolean kafkaBigDataFlag, Integer maxInc, Integer intervalInc, Integer endIncPartid, Integer startIncPartid) {
        this.kafkaBigDataFlag = kafkaBigDataFlag;
        this.maxInc = maxInc;
        this.intervalInc = intervalInc; 
        this.endIncPartid = endIncPartid;
        this.startIncPartid = startIncPartid;
    }

    public Boolean getKafkaBigDataFlag() {
        return kafkaBigDataFlag;
    }

    public Integer getMaxInc() {
        return maxInc;
    }

    public Integer getIntervalInc() {
        return intervalInc;
    }

    public Integer getEndIncPartid() {
        return endIncPartid;
    }

    public Integer getStartIncPartid() {
        return startIncPartid;
    }
}
