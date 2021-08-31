package it.luca.aurora.configuration.metadata;

import org.apache.hadoop.fs.Path;

public class FileNameRegexStrategyException extends Exception {

    public FileNameRegexStrategyException(String regex, Path filePath) {

        super(String.format("Unable to match file name %s with regex %s",
                filePath.getName(),
                regex));
    }
}
