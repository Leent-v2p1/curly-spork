package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities;

import java.util.Objects;

public class CtlStatisticInfo {

    public final String loadingId;
    public final String publicationDate;
    public final String entityId;
    public final String profile;
    public final String statisticValue;

    public CtlStatisticInfo(String loadingId,
                            String publicationDate,
                            String entityId,
                            String profile,
                            String statisticValue) {
        this.loadingId = loadingId;
        this.publicationDate = publicationDate;
        this.entityId = entityId;
        this.profile = profile;
        this.statisticValue = statisticValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CtlStatisticInfo)) {
            return false;
        }
        CtlStatisticInfo that = (CtlStatisticInfo) o;
        return Objects.equals(loadingId, that.loadingId) &&
                Objects.equals(publicationDate, that.publicationDate) &&
                Objects.equals(entityId, that.entityId) &&
                Objects.equals(statisticValue, that.statisticValue);
    }

    public String getLoadingId() {
        return loadingId;
    }

    public String getEntityId() {
        return entityId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(loadingId, publicationDate, entityId, profile, statisticValue);
    }
}
