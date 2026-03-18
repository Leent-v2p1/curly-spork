package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class FileMatchers {
    public static final PathMatcher IS_WORKFLOW_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:workflow.xml");
    public static final PathMatcher IS_XML_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:*.xml");
    public static final PathMatcher IS_YAML_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:*.{yaml,yml}");
    public static final PathMatcher IS_SQL_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:*.sql");
    public static final PathMatcher IS_SH_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:*.sh");
    public static final PathMatcher IS_PY_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:*.py");
    public static final PathMatcher IS_JSON_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:*.json");

    private static final Path EMPTY_PATH = Paths.get("");

    public static boolean contains(Path path, Path subpath) {
        List<Path> pathDirs = StreamSupport.stream(path.spliterator(), false).collect(toList());
        List<Path> sublistDirs = StreamSupport.stream(subpath.spliterator(), false).collect(toList());

        return subpath.equals(EMPTY_PATH) || Collections.indexOfSubList(pathDirs, sublistDirs) >= 0;
    }
}
