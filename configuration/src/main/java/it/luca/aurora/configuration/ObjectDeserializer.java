package it.luca.aurora.configuration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public class ObjectDeserializer {

    public enum DataFormat {
        JSON, YAML
    }

    private final static ObjectMapper jsonMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final static ObjectMapper yamlMapper = new YAMLMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Converts a file to instance of type [[T]]
     * @param file input file
     * @param tClass class of deserialized object
     * @param <T> type of deserialized object
     * @return instance of T
     * @throws IOException if deserialization fails
     */

    public static <T> T deserializeFile(File file, Class<T> tClass) throws IOException {

        String fileName = file.getName();
        String className = tClass.getSimpleName();
        ObjectMapper mapper = fileName.toLowerCase().endsWith(".json") ? jsonMapper : yamlMapper;
        log.info("Deserializing file {} as an instance of {}", fileName, className);
        T instance = mapper.readValue(file, tClass);
        log.info("Successfully deserialized file {} as an instance of {}", fileName, className);
        return instance;
    }


    /**
     * Converts a string to instance of type [[T]]
     * @param string input string
     * @param tClass class of deserialized object
     * @param dataFormat one value among {@link it.luca.aurora.configuration.ObjectDeserializer.DataFormat}
     * @param <T> type of deserialized object
     * @return instance of T
     * @throws IOException if deserialization fails
     */

    public static <T> T deserializeString(String string, Class<T> tClass, DataFormat dataFormat) throws IOException {

        String dataFormatName = dataFormat.name().toLowerCase();
        String className = tClass.getSimpleName();
        ObjectMapper mapper;
        switch (dataFormat) {
            case JSON: mapper = jsonMapper; break;
            case YAML: mapper = yamlMapper; break;
            default: throw new IllegalArgumentException(String.format("Unmatched %s: %s", DataFormat.class.getSimpleName(), dataFormatName));
        }

        log.info("Deserializing given {} string as an instance of {}", dataFormatName, className);
        T instance = mapper.readValue(string, tClass);
        log.info("Successfully deserialized given {} string as an instance of {}", dataFormatName, className);
        return instance;
    }
}
