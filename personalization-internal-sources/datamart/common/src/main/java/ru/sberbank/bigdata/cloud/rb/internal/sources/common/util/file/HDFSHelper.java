package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DatamartRuntimeException;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.AppPath.unixPath;

public class HDFSHelper {

    private static final Logger log = LoggerFactory.getLogger(HDFSHelper.class);

    public static int filesCount(String path) {
        try {
            FileStatus[] listFiles = getFileSystem().listStatus(new Path(path));
            log.info("list of files in path = {}: {}", path, listFiles);
            return listFiles.length;
        } catch (IOException e) {
            throw new DatamartRuntimeException(e);
        }
    }
    public static void createDirectory(String path) {
        createDirectory(new Path(path));
    }
    private static void createDirectory(Path path) {
        try {
            FileSystem hdfs = getFileSystem();
            if (!hdfs.exists(path)) {
                log.info("Create directory {}", path);
                hdfs.mkdirs(path);
            } else {
                log.warn("folder already exist {}", path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Exception during initialization of hdfs for creating directory " + path, e);
        }

    }

    /**
     * @param path path to directory in hdfs
     *
     * @return size of directory (without replication factor) in bytes or -1 if something went wrong
     */
    public static long getDirectorySize(String path) {
        try {
            return getFileSystem().getContentSummary(new Path(path)).getLength();
        } catch (IOException e) {
            log.error("Error during checking size of directory '" + path + "'.", e);
            return -1L;
        }
    }

    /**
     * @return empty string if file exist but empty
     */
    public static String readFileAsString(String path) {
        return String.join("\n", readFile(path));
    }

    public static List<String> readFile(String path) {
        try (FSDataInputStream is = getFileSystem().open(new Path(path));
             BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            return br.lines().collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Append data to file.
     * The file will be created if it doesn't exist
     *
     * @param data data that append to the file
     * @param path path to file in hdfs
     */
    public static void append(String data, String path) {
        if (HDFSHelper.isExist(new Path(path))) {
            final String previousData = HDFSHelper.readFileAsString(path);
            final String newFileState = String.join("\n", previousData, data);
            HDFSHelper.rewriteFile(newFileState, new Path(path));
        } else {
            HDFSHelper.writeFile(data, new Path(path));
        }
    }

    public static void deleteDirectory(String path) {
        delete(new Path(path), true);
    }

    public static void deleteFile(Path path) {
        delete(path, false);
    }

    private static void delete(Path path, boolean recursive) {
        try {
            FileSystem hdfs = getFileSystem();
            if (hdfs.exists(path)) {
                log.info("Deleting {}", path);
                hdfs.delete(path, recursive);
            } else {
                log.warn("folder doesn't exist {}", path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Exception during initialization of hdfs for deleting data in " + path, e);
        }
    }

    public static boolean isExist(Path path) {
        try {
            FileSystem hdfs = getFileSystem();
            if (hdfs.exists(path)) {
                log.info("Path exists: {}", path);
                return true;
            } else {
                log.warn("Path doesn't exist: {}", path);
                return false;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Exception during checking existence of hdfs path in " + path, e);
        }
    }

    public static void rewriteFile(String data, String path) {
        rewriteFile(data, new Path(path));
    }

    public static void rewriteFile(String data, java.nio.file.Path path) {
        rewriteFile(data, unixPath(path));
    }

    public static void rewriteFile(String stringToWrite, Path path) {
        deleteFile(path);
        writeFile(stringToWrite, path);
    }

    public static void writeFile(String data, String path) {
        writeFile(data, new Path(path));
    }

    public static void writeFile(String data, Path path) {
        if (!path.toString().startsWith("/")) {
            log.warn("Writing file with relative path {}", path);
        }
        try (OutputStream os = getFileSystem().create(path)) {
            os.write(data.getBytes());
        } catch (IOException e) {
            throw new UncheckedIOException("Exception during writing file to hdfs " + path, e);
        }
    }

    public static void moveFiles(String srcPath, String targetPath) {
        moveFiles(new Path(srcPath), new Path(targetPath));
    }

    public static void moveFiles(Path srcPath, Path targetPath) {
        try {
            moveFiles(getFileSystem(), srcPath, targetPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void moveFiles(FileSystem hdfs, Path srcPath, Path targetPath) throws IOException {
        log.info("hdfs = [{}], srcPath = [{}], targetPath = [{}]", hdfs, srcPath, targetPath);
        if (hdfs.exists(targetPath)) {
            for (FileStatus srcSubFile : hdfs.listStatus(srcPath)) {
                Path targetSubPath = new Path(targetPath, srcSubFile.getPath().getName());
                if (hdfs.exists(targetSubPath)) {
                    if (hdfs.isDirectory(targetSubPath)) {
                        moveFiles(hdfs, srcSubFile.getPath(), targetSubPath);
                    } else {
                        moveDuplicatedFile(hdfs, srcSubFile, targetSubPath, 1);
                    }
                } else {
                    hdfs.rename(srcSubFile.getPath(), targetSubPath);
                }
            }
        } else {
            final Path parent = targetPath.getParent();
            if (!hdfs.exists(parent)) {
                hdfs.mkdirs(parent);
            }
            hdfs.rename(srcPath, targetPath);
        }
    }

    private static void moveDuplicatedFile(FileSystem hdfs, FileStatus srcFile, Path targetSubPath, int i) throws IOException {
        log.info("hdfs = [{}], srcFile = [{}], targetSubPath = [{}], i = [{}]", hdfs, srcFile, targetSubPath, i);
        if (hdfs.exists(targetSubPath.suffix("_copy" + i))) {
            moveDuplicatedFile(hdfs, srcFile, targetSubPath, ++i);
        } else {
            hdfs.rename(srcFile.getPath(), targetSubPath.suffix("_copy" + i));
        }
    }

    private static FileSystem getFileSystem() {
        try {
            return FileSystem.get(new Configuration());
        } catch (IOException e) {
            throw new DatamartRuntimeException(e);
        }
    }

    public static FileStatus[] getDirectoryContent(Path dirPath) {
        FileSystem hdfs = HDFSHelper.getFileSystem();
        final FileStatus[] listFiles;
        try {
            listFiles = hdfs.listStatus(dirPath);
        } catch (IOException e) {
            throw new DatamartRuntimeException(e);
        }
        log.info("files from directory {}: {}", dirPath, Arrays.toString(listFiles));
        return listFiles;
    }

    public static List<String> getFileContentOrEmptyList(String pathToFile) {
        try {
            return readFile(pathToFile);
        } catch (Exception e) {
            log.info("File {} doesnt't exists, return empty String[]", pathToFile);
            return Collections.emptyList();
        }
    }

    public static void copyFiles(Path src, Path dst) {
        FileSystem hdfs = HDFSHelper.getFileSystem();
        log.info("Coping files: sourcePath = [{}], targetPath = [{}]", src, dst);
        try {
            final FileStatus[] fileStatuses = hdfs.listStatus(src);
            for (FileStatus fileStatus : fileStatuses) {
                FileUtil.copy(hdfs, fileStatus, hdfs, dst, false, true, new Configuration());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Exception during coping hdfs files from " + src + " to " + dst, e);
        }
    }

    public List<String> folderNamesRecursive(String path) {
        final FileStatus[] listFiles = getDirectoryContent(new Path(path));
        final List<String> folderNames = Arrays.stream(listFiles)
                .filter(FileStatus::isDirectory)
                .map(file -> file.getPath().getName())
                .filter(fileName -> !fileName.startsWith(".") && !fileName.startsWith("_"))
                .flatMap(folderName -> {
                    final List<String> subs = folderNamesRecursive(path + "/" + folderName);
                    return subs.isEmpty() ? Stream.of(folderName) : subs.stream().map(sub -> folderName + "/" + sub);
                })
                .collect(Collectors.toList());
        log.info("FolderNames in path {} are - {}", path, folderNames);
        return folderNames;
    }

    public long filesSize(String path) {
        try {
            return getFileSystem().getContentSummary(new Path(path)).getLength();
        } catch (IOException e) {
            log.error("Error during checking size of directory '" + path + "'.", e);
            throw new DatamartRuntimeException(e);
        }
    }
}
