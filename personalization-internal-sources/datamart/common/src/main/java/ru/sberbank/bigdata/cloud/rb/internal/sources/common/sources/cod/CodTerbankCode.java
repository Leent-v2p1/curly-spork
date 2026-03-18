package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;

public enum CodTerbankCode {

    private int code;

    CodTerbankCode(int code) {
        this.code = code;
    }

    public static CodTerbankCode getByCode(int terbankCode) {
        return stream(CodTerbankCode.values())
                .filter(code -> code.getCode() == terbankCode)
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }

    public static List<CodTerbankCode> listOfValues() {
        return asList(CodTerbankCode.values());
    }

    public int getCode() {
        return code;
    }
}
