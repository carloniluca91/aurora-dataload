package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

import java.util.Objects;

@Getter
public class Load {

    private final PartitionInfo partitionInfo;
    private final Target target;

    @JsonCreator
    public Load(@JsonProperty(JsonField.PARTITION_INFO) PartitionInfo partitionInfo,
                @JsonProperty(JsonField.TARGET) Target target) {

        this.partitionInfo = Objects.requireNonNull(partitionInfo, JsonField.PARTITION_INFO);
        this.target = Objects.requireNonNull(target, JsonField.TARGET);
    }
}
