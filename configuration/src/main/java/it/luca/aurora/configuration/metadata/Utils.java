package it.luca.aurora.configuration.metadata;

import java.util.Map;
import java.util.NoSuchElementException;

public class Utils {

    /**
     * Retrieve value of given key from input map or throws exception
     * @param map input map
     * @param key input key
     * @return value of given key
     * @throws NoSuchElementException if given key is not present
     */

    public static String getValueOrThrowException(Map<String, String> map, String key) {

        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            throw new NoSuchElementException(String.format("Key %s not found", key));
        }
    }
}
