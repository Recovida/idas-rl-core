package com.cidacs.rl.record;

public class ColumnRecordModel {
    private String id;
    private String type;
    private String value;
    private String originalValue;

    public ColumnRecordModel(String id, String type, String value) {
        this(id, type, value, value);
    }

    public ColumnRecordModel(String id, String type, String value, String originalValue) {
        this.id = id;
        this.type = type;
        this.value = value;
        this.originalValue = originalValue;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOriginalValue() {
        return originalValue;
    }

    public void setOriginalValue(String originalValue) {
        this.originalValue = originalValue;
    }
}
