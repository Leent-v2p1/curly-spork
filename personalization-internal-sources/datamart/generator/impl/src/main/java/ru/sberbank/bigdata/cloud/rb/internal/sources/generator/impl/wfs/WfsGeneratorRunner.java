package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.wfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.environment.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.SneakyThrowBiConsumer.sneakyThrow;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.CtlWorkflowName.getWfsFileName;

public class WfsGeneratorRunner {
    private static final Logger log = LoggerFactory.getLogger(WfsGeneratorRunner.class);

    public static void generateWfsYaml(Environment environment, Path stageBaseAppPath) throws IOException {
        final WfsGenerator wfsGenerator = new WfsGenerator(environment);
        final Map<String, String> aliasWfConfigMap = wfsGenerator.generate();
        Files.createDirectories(stageBaseAppPath);
        aliasWfConfigMap
                .forEach(sneakyThrow((alias, configContent) -> {
                    String fileName = getWfsFileName(environment, alias);
                    Path path = stageBaseAppPath.resolve(fileName);
                            log.info("Writing '{}' wfs.yaml to path {}", alias, path);
                            Files.write(path, configContent.getBytes());
                        })
                );
    }
}
