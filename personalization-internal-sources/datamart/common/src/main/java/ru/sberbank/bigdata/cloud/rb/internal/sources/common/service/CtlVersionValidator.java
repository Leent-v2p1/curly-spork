package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service;

import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCalls;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV1;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.CtlApiCallsV5;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client.dto.CtlInfo;

import java.io.IOException;
import java.util.Objects;
import java.util.StringTokenizer;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.PropertyConstants.DEFAULT_VALUE_CTL_VERSION_V5;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.getSystemProperty;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool.safeSystemProperty;

public class CtlVersionValidator {

    private static final Logger log = LoggerFactory.getLogger(CtlVersionValidator.class);
    private static final String NO_DIGITS_AND_POINTS = "[^\\d.]*"; // все символы кроме цифр и '.'
    private CtlApiCalls ctlApiCalls;

    public CtlVersionValidator(CtlApiCalls ctlApiCalls) {
        this.ctlApiCalls = ctlApiCalls;
    }
    public static void main(String[] args) throws JSONException, IOException, TypeErrorException {
        CtlApiCalls ctlApiCalls;
        String ctlUrl = safeSystemProperty("ctl.url");
        String requiredCtlVersion = safeSystemProperty("ctl.version.required");
        boolean ctlApiVersionV5 = Boolean.parseBoolean(getSystemProperty("spark.ctl.api.version.v5", DEFAULT_VALUE_CTL_VERSION_V5));
        log.info("Ctl version use v5: {}", ctlApiVersionV5);
        if (ctlApiVersionV5){
            ctlApiCalls = new CtlApiCallsV5(ctlUrl);
        } else {
            ctlApiCalls = new CtlApiCallsV1(ctlUrl);
        }
        CtlVersionValidator validator = new CtlVersionValidator(ctlApiCalls);
        validator.validate(requiredCtlVersion, validator.getCtlVersion());
    }

    public void validate(String requiredCtlVersion, String actualCtlVersion) {
        SemVerComparable required = new SemVerComparable(requiredCtlVersion);
        SemVerComparable actual = new SemVerComparable(parseVersion(actualCtlVersion));
        if (required.compareTo(actual) > 0) {
            throw new IllegalStateException(String.format("Required actualCtlVersion is - %s, Actual is - %s", requiredCtlVersion, actualCtlVersion));
        }
    }

    private String parseVersion(String version) {
        return version.replaceAll(NO_DIGITS_AND_POINTS, "");
    }

    String getCtlVersion() throws JSONException, IOException, TypeErrorException {
        if (ctlApiCalls instanceof CtlApiCallsV1){
            final CtlInfo ctlInfo = ctlApiCalls.getCtlInfo();
            log.info("ctlInfo = {}", ctlInfo);
            return ctlInfo.getVersion();
        } else if (ctlApiCalls instanceof CtlApiCallsV5){
            final String ctlInfo = ctlApiCalls.getCtlInfo();
            log.info("ctlInfo = {}", ctlInfo);
            return ctlInfo;
        } else {
            throw new TypeErrorException("Ctl api class is not valid");
        }
    }

    private class SemVerComparable implements Comparable {
        private final int major;
        private final int minor;
        private final int patch;

        private SemVerComparable(String version) {
            StringTokenizer tokenizer = new StringTokenizer(version, ".");
            major = Integer.parseInt(tokenizer.nextToken());
            minor = Integer.parseInt(tokenizer.nextToken());
            patch = Integer.parseInt(tokenizer.nextToken());
        }

        @Override
        public int compareTo(Object o) {
            if (o instanceof SemVerComparable) {
                SemVerComparable that = (SemVerComparable) o;
                if (major != that.major) {
                    //Изменение мажорной версии может вести к отсутствию необходимых функций
                    return 1;
                } else {
                    if (minor > that.minor) {
                        return 1;
                    } else if (minor < that.minor) {
                        return -1;
                    } else {
                        return Integer.compare(patch, that.patch);
                    }
                }
            }
            throw new IllegalStateException("must only compare SemVerComparable");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SemVerComparable that = (SemVerComparable) o;
            return major == that.major &&
                    minor == that.minor &&
                    patch == that.patch;
        }

        @Override
        public int hashCode() {
            return Objects.hash(major, minor, patch);
        }
    }
}
