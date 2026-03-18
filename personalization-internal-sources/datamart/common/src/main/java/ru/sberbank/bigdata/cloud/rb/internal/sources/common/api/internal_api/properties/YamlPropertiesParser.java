package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.lineSeparator;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.StringUtils.isNotBlank;

public class YamlPropertiesParser {
    private static final Logger log = LoggerFactory.getLogger(YamlPropertiesParser.class);
    private final Yaml yaml;

    public YamlPropertiesParser(LoaderOptions loaderOptions) {
        final DumperOptions dumperOptions = createDumperOptions();
        Representer representer = createRepresenter();

        this.yaml = new Yaml(new Constructor(), representer, dumperOptions, loaderOptions);
    }

    public YamlPropertiesParser() {
        final DumperOptions dumperOptions = createDumperOptions();
        Representer representer = createRepresenter();

        this.yaml = new Yaml(new Constructor(), representer, dumperOptions);
    }

    private DumperOptions createDumperOptions() {
        final DumperOptions dumperOptions = new DumperOptions();
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        dumperOptions.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
        dumperOptions.setIndent(4);
        dumperOptions.setIndicatorIndent(2);

        return dumperOptions;
    }

    private Representer createRepresenter() {
        return new Representer() {
            @Override
            protected Node representScalar(Tag tag, String value, Character style) {
                if (value.equals("null")) {
                    return super.representScalar(tag, "", style);
                }
                return super.representScalar(tag, value, style);
            }
        };
    }

    public Map<String, Object> parseString(String fileContent) {
        @SuppressWarnings("unchecked")
        Map<String, Object> yamlConfig = (Map<String, Object>) yaml.load(fileContent);
        return process(yamlConfig);
    }

    public Map<String, Object> parse(String fileName) {
        @SuppressWarnings("unchecked")
        Map<String, Object> yamlConfig =
                (Map<String, Object>) yaml.load(YamlPropertiesParser.class.getResourceAsStream(fileName));
        return process(yamlConfig);
    }

    public Map<String, Object> parse(File path) {
        String yml = loadStringYaml(path);
        YamlPropertiesParser yamlParser = new YamlPropertiesParser();
        return yamlParser.parseString(yml);
    }

    public Map<String, Object> parseRawMap(Path path) {
        String ymlContent = loadStringYaml(path.toFile());
        @SuppressWarnings("unchecked")
        Map<String, Object> yamContentMap = (Map<String, Object>) yaml.load(ymlContent);
        return yamContentMap;
    }

    public Map<String, Object> parseRawMap(String fileName) {
        @SuppressWarnings("unchecked")
        Map<String, Object> yamlConfig =
                (Map<String, Object>) yaml.load(YamlPropertiesParser.class.getResourceAsStream(fileName));
        return yamlConfig;
    }

    public void saveYamlMap(Map<String, Object> yamlMap, Path path) {
        try {
            String result = yaml.dump(yamlMap);
            final Path parent = path.getParent();
            boolean mkdirsFailed = !parent.toFile().mkdirs();
            if (mkdirsFailed) {
                log.info("Directory {} already exists", parent);
            }
            Files.write(path, result.getBytes(), CREATE, WRITE, TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String loadStringYaml(File file) {
        try (Stream<String> linesStream = Files.lines(file.toPath())) {
            return linesStream
                    .collect(Collectors.joining(lineSeparator()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, Object> process(Map<String, Object> yamlConfig) {
        Map<String, Object> result = new LinkedHashMap<>();
        buildFlattenedMap(result, yamlConfig, null);
        return result;
    }

    private void buildFlattenedMap(Map<String, Object> result, Map<String, Object> source, String path) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            if (isNotBlank(path)) {
                key = path + "." + key;
            }
            Object value = entry.getValue();
            if (value instanceof String
                    || value instanceof Integer
                    || value instanceof Boolean
                    || value instanceof Double) {
                result.put(key, value);
            } else if (value instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> list = (List<String>) value;
                result.put(key, list);
            } else if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) value;
                buildFlattenedMap(result, map, key);
            } else {
                throw new IllegalArgumentException("Unknown type of value " + value + ", key = " + key);
            }
        }
    }
}
