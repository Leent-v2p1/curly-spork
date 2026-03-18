package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.internal;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.ToDeleteDir;

import java.time.LocalDate;
import java.util.Optional;

public class ReservingParameters {
    private final ToDeleteDir dirToDelete;
    private final Optional<LocalDate> recoveryDate;
    private final boolean recoveryMode;
    private final boolean firstLoading;
    private final boolean testPartiallyHistoryUpdate;
    private final boolean isRepartitionNeeded;
    private final LocalDate buildDate;

    public ReservingParameters(ToDeleteDir dirToDelete,
                               Optional<LocalDate> recoveryDate,
                               boolean recoveryMode,
                               boolean firstLoading,
                               boolean testPartiallyHistoryUpdate,
                               boolean isRepartitionNeeded,
                               LocalDate buildDate) {
        this.dirToDelete = dirToDelete;
        this.recoveryDate = recoveryDate;
        this.recoveryMode = recoveryMode;
        this.firstLoading = firstLoading;
        this.testPartiallyHistoryUpdate = testPartiallyHistoryUpdate;
        this.isRepartitionNeeded = isRepartitionNeeded;
        this.buildDate = buildDate;
    }

    public ReservingParameters() {
        dirToDelete = null;
        recoveryDate = null;
        buildDate = null;
        recoveryMode = false;
        firstLoading = false;
        testPartiallyHistoryUpdate = false;
        isRepartitionNeeded = false;
    }

    public ToDeleteDir getDirToDelete() {
        return dirToDelete;
    }

    public Optional<LocalDate> getRecoveryDate() {
        return recoveryDate;
    }

    public boolean isRecoveryMode() {
        return recoveryMode;
    }

    public boolean isFirstLoading() {
        return firstLoading;
    }

    public boolean isTestPartiallyHistoryUpdate() {
        return testPartiallyHistoryUpdate;
    }

    public boolean isRepartitionNeeded() {
        return isRepartitionNeeded;
    }

    public LocalDate getBuildDate() {
        return buildDate;
    }
}
