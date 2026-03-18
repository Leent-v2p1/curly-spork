package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util;

public class DatamartRuntimeException extends RuntimeException {

    public DatamartRuntimeException() {
        super();
    }

    public DatamartRuntimeException(String message) {
        super(message);
    }

    public DatamartRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatamartRuntimeException(Throwable cause) {
        super(cause);
    }

    protected DatamartRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
