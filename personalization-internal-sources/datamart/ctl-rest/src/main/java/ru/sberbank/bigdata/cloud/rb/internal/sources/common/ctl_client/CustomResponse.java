package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client;

import java.util.function.Consumer;

public class CustomResponse<T> {
    private final T response;
    private final int code;
    private final String errorMessage;
    private final String uri;
    private final String method;
    private final String requestBody;

    private CustomResponse(T response, int code, String errorMessage, String uri, String method, String requestBody) {
        this.response = response;
        this.code = code;
        this.errorMessage = errorMessage;
        this.uri = uri;
        this.method = method;
        this.requestBody = requestBody;
    }

    public static <T> CustomResponse<T> success(T response, int code) {
        return new CustomResponse<>(response, code, null, null, null, null);
    }

    public static <T> CustomResponse<T> error(int code, String errorMessage, String uri, String method, String requestBody) {
        return new CustomResponse<>(null, code, errorMessage, uri, method, requestBody);
    }

    public boolean isSuccess() {
        return code >= 200 && code < 300;
    }

    public void ifSuccess(Consumer<? super T> consumer) {
        consumer.accept(response);
    }

    public T ifErrorThrowException() {
        if (isSuccess()) {
            return response;
        } else {
            throw new IllegalStateException(String.format("Error code = %d, errorMessage = `%s`, while executing request URI = %s, method = %s, requestBody = `%s`", code, errorMessage, uri, method, requestBody));
        }
    }

    public T response() {
        return response;
    }

    public int code() {
        return code;
    }

    public String errorMessage() {
        return errorMessage;
    }
}
