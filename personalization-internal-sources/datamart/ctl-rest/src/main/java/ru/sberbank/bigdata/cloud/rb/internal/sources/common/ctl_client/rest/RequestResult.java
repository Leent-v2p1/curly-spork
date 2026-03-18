package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.rest;

public class RequestResult {
    private int responseCode;
    private String resposeText;

    public RequestResult(int responseCode, String resposeText) {
        this.responseCode = responseCode;
        this.resposeText = resposeText;
    }

    public int getResponseCode() {
        return this.responseCode;
    }

    public String getResposeText() {
        return this.resposeText;
    }
}

