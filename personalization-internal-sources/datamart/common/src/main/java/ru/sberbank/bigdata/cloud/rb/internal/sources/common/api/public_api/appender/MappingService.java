package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.mapping.EpkMapping;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;

import java.util.List;

/**
 * Class contains method to resolve MdmIdMapping for id of clients in MDM and EPK
 * Mapping step creates table for specific source system(EKP, WAY4) and usually done as a separate stage datamart(Way4MdmMapping)
 */
public class MappingService {

    private final DatamartContext datamartContext;

    public MappingService(DatamartContext datamartContext) {
        this.datamartContext = datamartContext;
    }

    /*EPK id mapping*/

    public EpkMapping epkIdFromEpkSystem(List<EpkSystemCode> systemCodes) {
        return new EpkMapping(datamartContext, systemCodes);
    }
}
