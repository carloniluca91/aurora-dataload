package it.luca.aurora.configuration.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.luca.aurora.configuration.metadata.JsonField;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

@Getter
public class Transform {

    private final List<String> filters;
    private final List<String> transformations;
    private final List<String> dropDuplicates;
    private final List<String> dropColumns;

    @JsonCreator
    public Transform(@JsonProperty(JsonField.FILTERS) List<String> filters,
                     @JsonProperty(JsonField.TRANSFORMATIONS) List<String> transformations,
                     @JsonProperty(JsonField.DROP_DUPLICATES) List<String> dropDuplicates,
                     @JsonProperty(JsonField.DROP_COLUMNS) List<String> dropColumns) {

        this.filters = filters;
        this.transformations = transformations;
        this.dropDuplicates = dropDuplicates;
        this.dropColumns = dropColumns;
    }

    protected boolean nullOrEmptyList(List<?> list) {

        return !Optional.ofNullable(list).isPresent() || list.isEmpty();
    }

    public boolean noFiltersToApply() {
        return nullOrEmptyList(filters);
    }

    public boolean noTransformationsToApply() {
        return nullOrEmptyList(transformations);
    }

    public boolean noDuplicatesToDrop() {
        return nullOrEmptyList(dropDuplicates);
    }

    public boolean noColumnsToDrop() {
        return nullOrEmptyList(dropColumns);
    }
}
