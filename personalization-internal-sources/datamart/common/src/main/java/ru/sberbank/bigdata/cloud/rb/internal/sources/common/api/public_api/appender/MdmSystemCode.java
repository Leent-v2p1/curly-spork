package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender;

import java.util.stream.Stream;

public enum MdmSystemCode {

    private int code;

    MdmSystemCode(int code) {
        this.code = code;
    }

    public static MdmSystemCode[] getCodCodeArray() {
        return Stream.of(MdmSystemCode.values())
                .filter(code -> code.toString().startsWith("COD_"))
                .toArray(MdmSystemCode[]::new);
    }

    public int getCode() {
        return code;
    }
}
