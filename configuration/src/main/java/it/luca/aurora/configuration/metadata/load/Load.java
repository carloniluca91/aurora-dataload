package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

import java.util.Map;

import static it.luca.aurora.configuration.metadata.Utils.getValueOrThrowException;
import static java.util.Objects.requireNonNull;

public class Load {

    public static final String TRUSTED = "trusted";
    public static final String ERROR = "error";

    @Getter
    private final PartitionInfo partitionInfo;
    private final Map<String, String> target;

    @JsonCreator
    public Load(@JsonProperty(JsonField.PARTITION_INFO) PartitionInfo partitionInfo,
                @JsonProperty(JsonField.TARGET) Map<String, String> target) {

        this.partitionInfo = requireNonNull(partitionInfo, JsonField.PARTITION_INFO);
        this.target = requireNonNull(target, JsonField.TARGET);
    }

    /**
     * Get fully qualified name of trusted table
     * @return fully qualified name of trusted table
     */

    public String getTrustedTableName() {
        return getValueOrThrowException(target, TRUSTED);
    }

    /**
     * Get fully qualified name of error table
     * @return fully qualified name of error table
     */

    public String getErrorTableName() {
        return getValueOrThrowException(target, ERROR);
    }
}
