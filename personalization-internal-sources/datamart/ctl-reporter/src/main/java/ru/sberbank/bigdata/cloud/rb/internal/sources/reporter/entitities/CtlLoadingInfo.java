package ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.entitities;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.reporter.reporters.CtlLoadingsReporter.CTL_TIME_FORMAT;

public class CtlLoadingInfo implements Comparable<CtlLoadingInfo> {
    public final String loadingId;
    public final String wfId;
    public final String wfSystem;
    public final String wfName;
    public final String alive;
    public final String status;
    public final String startLoading;
    public final String startRunning;
    public final String endLoading;
    public final String duration;
    public final String reportDate;

    public CtlLoadingInfo(String loadingId,
                          String wfId,
                          String wfSystem,
                          String wfName,
                          String alive,
                          String status,
                          String startLoading,
                          String startRunning,
                          String endLoading,
                          String duration,
                          String reportDate) {
        this.loadingId = loadingId;
        this.wfId = wfId;
        this.wfSystem = wfSystem;
        this.wfName = wfName;
        this.alive = alive;
        this.status = status;
        this.startLoading = startLoading;
        this.startRunning = startRunning;
        this.endLoading = endLoading;
        this.duration = duration;
        this.reportDate = reportDate;
    }

    public CtlLoadingInfo copyWithReportDate(LocalDate reportDate) {
        return new CtlLoadingInfo(loadingId,
                wfId,
                wfSystem,
                wfName,
                alive,
                status,
                startLoading,
                startRunning,
                endLoading,
                duration,
                reportDate.format(ISO_LOCAL_DATE));
    }

    /**
     * Order of rows in a report depends on compareTo method
     */
    @Override
    public int compareTo(CtlLoadingInfo other) {
        final int systemCompare = wfSystem.compareTo(other.wfSystem);
        if (systemCompare != 0) {
            return systemCompare;
        }
        final LocalDateTime startLoad = LocalDateTime.parse(this.startLoading, CTL_TIME_FORMAT);
        final LocalDateTime otherStartLoad = LocalDateTime.parse(other.startLoading, CTL_TIME_FORMAT);
        if (startRunning != null && other.startRunning != null) {
            final LocalDateTime startRun = LocalDateTime.parse(this.startRunning, CTL_TIME_FORMAT);
            final LocalDateTime otherStartRun = LocalDateTime.parse(other.startRunning, CTL_TIME_FORMAT);
            final int runCompareResult = startRun.compareTo(otherStartRun);
            if (runCompareResult != 0) {
                return runCompareResult;
            }
        } else if (other.startRunning != null) {
            return 1;
        } else if (startRunning != null) {
            return -1;
        }

        final int loadCompareResult = startLoad.compareTo(otherStartLoad);
        if (loadCompareResult != 0) {
            return loadCompareResult;
        }
        final LocalDate reportDay = LocalDate.parse(this.reportDate, ISO_LOCAL_DATE);
        final LocalDate otherReportDay = LocalDate.parse(other.reportDate, ISO_LOCAL_DATE);
        return reportDay.compareTo(otherReportDay);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CtlLoadingInfo loadingInfo = (CtlLoadingInfo) o;
        return Objects.equals(loadingId, loadingInfo.loadingId) &&
                Objects.equals(wfId, loadingInfo.wfId) &&
                Objects.equals(wfSystem, loadingInfo.wfSystem) &&
                Objects.equals(wfName, loadingInfo.wfName) &&
                Objects.equals(alive, loadingInfo.alive) &&
                Objects.equals(status, loadingInfo.status) &&
                Objects.equals(startLoading, loadingInfo.startLoading) &&
                Objects.equals(startRunning, loadingInfo.startRunning) &&
                Objects.equals(endLoading, loadingInfo.endLoading) &&
                Objects.equals(duration, loadingInfo.duration) &&
                Objects.equals(reportDate, loadingInfo.reportDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(loadingId, wfId, wfSystem, wfName, alive, status, startLoading, startRunning, endLoading, duration, reportDate);
    }

    @Override
    public String toString() {
        return "CtlLoadingInfo{" +
                "loadingId='" + loadingId + '\'' +
                ", wfId='" + wfId + '\'' +
                ", wfSystem='" + wfSystem + '\'' +
                ", wfName='" + wfName + '\'' +
                ", alive='" + alive + '\'' +
                ", status='" + status + '\'' +
                ", startLoading='" + startLoading + '\'' +
                ", startRunning='" + startRunning + '\'' +
                ", endLoading='" + endLoading + '\'' +
                ", duration='" + duration + '\'' +
                ", reportDate='" + reportDate + '\'' +
                '}';
    }
}
