package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.develop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.BuildProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.copier.DefaultBuildFileCopier;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.ProductionBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.production.test.ProductionTestBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultBuild {
    private static final Logger log = LoggerFactory.getLogger(DefaultBuild.class);

    public static void main(String[] args) throws IOException {
        final List<String> activeProfiles = Arrays.stream(System.getProperty("activeProfiles").split(","))
                .filter(profile -> !profile.startsWith("${"))
                .collect(Collectors.toList());
        if (activeProfiles.isEmpty()) {
            log.warn("activeProfiles is empty {} ", activeProfiles);
        }
        log.info("activeProfiles = {}", activeProfiles);

        final BuildProperties buildProperties = new BuildProperties();
        buildProperties.setBuildDirSysProperty();

        DefaultBuildFileCopier.copy(buildProperties.buildTimestamp, buildProperties.projectBaseDir);

        if (activeProfiles.contains(Environment.PRODUCTION.nameLowerCase())) {
            ProductionBuilder.build();
        }
        if (activeProfiles.contains(Environment.PRODUCTION_TEST.nameLowerCase())) {
            ProductionTestBuilder.build();
        }
    }
}
