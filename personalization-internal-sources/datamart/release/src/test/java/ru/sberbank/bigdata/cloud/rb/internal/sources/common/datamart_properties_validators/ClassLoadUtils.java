package ru.sberbank.bigdata.cloud.rb.internal.sources.common.datamart_properties_validators;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

class ClassLoadUtils {
    private static final String PROJECT_PACKAGE = "ru.sberbank.bigdata.cloud.rb.internal.sources.datamart";

    static List<Class<?>> superClasses(Class<?> aClass) {
        final List<Class<?>> superClasses = new ArrayList<>();
        Class<?> superClass = aClass.getSuperclass();
        while (superClass != Object.class) {
            superClasses.add(superClass);
            superClass = superClass.getSuperclass();
        }
        return superClasses;
    }

    static List<Class<?>> findClasses() throws IOException, ClassNotFoundException {
        final List<Class<?>> classes = new ArrayList<>();
        final String classPath = System.getProperty("java.class.path");
        final String[] paths = classPath.split(File.pathSeparator);
        for (String path : paths) {
            final File file = new File(path);
            if (file.getPath().endsWith(".jar")) {
                classes.addAll(findClassesInJar(file));
            } else {
                classes.addAll(findClasses("", file.listFiles()));
            }
        }
        return classes;
    }

    private static List<Class<?>> findClassesInJar(File file) throws IOException, ClassNotFoundException {
        final List<Class<?>> classes = new ArrayList<>();

        final Enumeration<JarEntry> entries = new JarFile(file).entries();
        while (entries.hasMoreElements()) {
            final JarEntry entry = entries.nextElement();
            final String name = entry.getName();
            if (name.endsWith(".class")) {
                final String className = name.replace("/", ".").replace(".class", "");
                loadClass(className).ifPresent(classes::add);
            }
        }
        return classes;
    }

    private static List<Class<?>> findClasses(String basePackage, File[] files) throws ClassNotFoundException {
        final List<Class<?>> classes = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                classes.addAll(findClasses((basePackage.isEmpty() ? "" : basePackage + ".") + file.getName(), file.listFiles()));
            } else {
                final String className = basePackage + "." + file.getName().replace(".class", "");
                loadClass(className).ifPresent(classes::add);
            }
        }
        return classes;
    }

    private static Optional<Class<?>> loadClass(String className) throws ClassNotFoundException {
        if (className.startsWith(PROJECT_PACKAGE)) {
            final Class<?> aClass = Class.forName(className);
            return Optional.of(aClass);
        } else {
            return Optional.empty();
        }
    }
}
