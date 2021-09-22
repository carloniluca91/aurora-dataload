package it.luca.aurora.configuration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class ObjectDeserializer {

    private final static ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Converts a {@link File} to instance of type [[T]]
     * @param file input file
     * @param tClass class of deserialized object
     * @param <T> type of deserialized object
     * @return instance of T
     * @throws IOException if deserialization fails
     */

    public static <T> T deserializeFile(File file, Class<T> tClass) throws IOException {

        String fileName = file.getName();
        String className = tClass.getSimpleName();
        log.info("Deserializing file {} as an instance of {}", fileName, className);
        T instance = mapper.readValue(file, tClass);
        log.info("Successfully deserialized file {} as an instance of {}", fileName, className);
        return instance;
    }

    /**
     * Converts a {@link InputStream} to instance of type [[T]]
     * @param stream input stream
     * @param tClass class of deserialized object
     * @param <T> type of deserialized object
     * @return instance of T
     * @throws IOException if deserialization fails
     */

    public static <T> T deserializeStream(InputStream stream, Class<T> tClass) throws IOException {

        String streamClassName = stream.getClass().getSimpleName();
        String className = tClass.getSimpleName();
        log.info("Deserializing input {} as an instance of {}", streamClassName, className);
        T instance = mapper.readValue(stream, tClass);
        log.info("Successfully deserialized input {} as an instance of {}", streamClassName, className);
        return instance;
    }

    /**
     * Converts a {@link String} to instance of type [[T]]
     * @param string input string
     * @param tClass class of deserialized objecT
     * @param <T> type of deserialized object
     * @return instance of T
     * @throws IOException if deserialization fails
     */

    public static <T> T deserializeString(String string, Class<T> tClass) throws IOException {

        String className = tClass.getSimpleName();
        log.info("Deserializing given string as an instance of {}", className);
        T instance = mapper.readValue(string, tClass);
        log.info("Successfully deserialized given string as an instance of {}", className);
        return instance;
    }
}
