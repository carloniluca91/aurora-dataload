package it.luca.aurora.configuration.metadata.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

@Getter
public class Load {

    private final PartitionInfo partitionInfo;
    private final Target target;

    @JsonCreator
    public Load(@JsonProperty(JsonField.PARTITION_INFO) PartitionInfo partitionInfo,
                @JsonProperty(JsonField.TARGET) Target target) {

        this.partitionInfo = partitionInfo;
        this.target = target;
    }
}
