package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;

public enum EpkSystemCode {


    private final int code;

    EpkSystemCode(int code) {
        this.code = code;
    }

    public static EpkSystemCode getByCode(int epkCode) {
        return stream(EpkSystemCode.values())
                .filter(code -> code.getCode() == epkCode)
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }

    public static List<EpkSystemCode> listOfValues() {
        return asList(EpkSystemCode.values());
    }

    public static EpkSystemCode[] getCodCodeArray() {
        return Stream.of(EpkSystemCode.values())
                .filter(code -> code.toString().startsWith("COD_"))
                .toArray(EpkSystemCode[]::new);
    }

    public int getCode() {
        return code;
    }
}
