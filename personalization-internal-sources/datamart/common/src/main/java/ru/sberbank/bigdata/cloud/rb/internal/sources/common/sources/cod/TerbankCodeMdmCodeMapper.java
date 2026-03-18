package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.MdmSystemCode;

import java.util.EnumMap;
import java.util.Map;

public class TerbankCodeMdmCodeMapper {
    private static final Map<CodTerbankCode, MdmSystemCode> mapping = new EnumMap<>(CodTerbankCode.class);

    static {

    }

    public MdmSystemCode map(CodTerbankCode codTerbankCode) {
        return mapping.get(codTerbankCode);
    }
}
