package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.CollectionUtils.transformToMutableList;

public class ActionConfReader {
    public List<ActionConf> read(Path path) {
        final Representer representer = new Representer();
        representer.getPropertyUtils().setSkipMissingProperties(true);
        final Yaml yaml = new Yaml(representer);
        try {
            return transformToMutableList(Arrays.asList(yaml.loadAs(new FileReader(path.toFile()), ActionConf[].class)));
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Map<String, ActionConf> asMap(List<? extends ActionConf> actionConfList) {
        return actionConfList.stream().collect(toMap((Function<ActionConf, String>) ActionConf::getName, identity()));
    }
}
