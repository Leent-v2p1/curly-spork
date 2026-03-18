package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.ddl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonSchemaSerializer implements SchemaSerializer {
    public String serialize(String tableName, StructType schema) {
        return tableName + "\t" + Stream.of(schema.fields())
                .map(f -> String.format("{\"name\":\"%s\",\"type\":\"%s\"}", f.name(), f.dataType().toString()))
                .collect(Collectors.joining(",", "[", "]\n"));
    }

    public StructType deserialize(String representation) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final CollectionType type = mapper.getTypeFactory().constructCollectionType(List.class, Map.class);
        final List<Map<String, String>> schema = mapper.readValue(representation, type);
        final StructField[] fields = schema.stream()
                .map(map -> new StructField(
                        map.get("name"),
                        getTypeFromString(map.get("type")),
                        true,
                        Metadata.empty())
                ).toArray(StructField[]::new);
        return new StructType(fields);
    }

    /**
     * Earlier we used method DataType.fromCaseClassString which transformed string representation to class of data type.
     * Examples:
     * "DecimalType(19,0)" ->  DecimalType
     * But this method was deprecated, now we use nameToType method from object of DataType(object is a singleton at Scala at compile time)
     * To do this we should use reflection because it is not public api. Also input format of string representation is changed,
     * so we change it in this method.
     * Examples:
     * "DecimalType(19,0)" -> "decimal(19,0)" -> DecimalType
     *
     * P.S. yes, we could migrate to DataType.fromJson() and StructField.json(), but it require to generate all schemas from scratch
     */
    private DataType getTypeFromString(String stringClassRepresentation) {
        try {
            final String oldFormatTypeName = stringClassRepresentation.replace("Type", "").toLowerCase();
            final Class<?> singletonClass = this.getClass().getClassLoader().loadClass("org.apache.spark.sql.types.DataType$");
            final Field singletonInstanceField = singletonClass.getDeclaredField("MODULE$");
            singletonInstanceField.setAccessible(true);
            final Object singletonInstance = singletonInstanceField.get(singletonClass);
            final Method methodToGetInstanceOfClass = singletonInstance.getClass().getDeclaredMethod("nameToType", String.class);
            methodToGetInstanceOfClass.setAccessible(true);
            return (DataType) methodToGetInstanceOfClass.invoke(singletonInstance, oldFormatTypeName);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }
}
