package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.write;

public class FileContent {

    private final String name;

    private final String content;

    public FileContent(String name, String content) {
        this.name = name;
        this.content = content;
    }

    public String fileName() {
        return name;
    }

    public String content() {
        return content;
    }

    @Override
    public String toString() {
        return "FileContent{" +
                "name='" + name + '\'' +
                '}';
    }
}
