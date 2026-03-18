package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.cod;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.EpkSystemCode;

import java.util.*;

public class TerbankCodeEpkCodeMapper {
    private static final Map<CodTerbankCode, List<EpkSystemCode>> mapping = new EnumMap<>(CodTerbankCode.class);

    static {
    }

    public List<EpkSystemCode> map(CodTerbankCode codTerbankCode) {
        return mapping.get(codTerbankCode);
    }
}
