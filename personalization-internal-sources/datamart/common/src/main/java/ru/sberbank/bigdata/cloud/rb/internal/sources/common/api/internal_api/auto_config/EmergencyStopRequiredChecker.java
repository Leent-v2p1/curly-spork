package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config;

/**
 * Класс проверяет необходимость механизме стоп-крана
 * Флагом является значение ctl параметра (или параметра в system.conf)
 * spark.datamart.emergencyStop = true
 */
public class EmergencyStopRequiredChecker {

    private final boolean emergencyStopRequired;

    public EmergencyStopRequiredChecker(boolean emergencyStopRequired) {
        this.emergencyStopRequired = emergencyStopRequired;
    }

    public boolean isEmergencyStopRequired() {
        return emergencyStopRequired;
    }
}
