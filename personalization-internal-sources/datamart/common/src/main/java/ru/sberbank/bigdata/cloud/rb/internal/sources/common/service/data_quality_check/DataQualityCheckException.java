package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.data_quality_check;

public class DataQualityCheckException extends RuntimeException {
    public DataQualityCheckException() {
        super();
    }

    public DataQualityCheckException(String message) {
        super(message);
    }
}
