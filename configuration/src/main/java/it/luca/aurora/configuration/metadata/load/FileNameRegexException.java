package it.luca.aurora.configuration.metadata.load;

import org.apache.hadoop.fs.Path;

public class FileNameRegexException extends Exception {

    public FileNameRegexException(String regex, Path filePath) {

        super(String.format("Unable to match file name %s with regex %s",
                filePath.getName(),
                regex));
    }
}
