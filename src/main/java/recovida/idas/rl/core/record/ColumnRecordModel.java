package recovida.idas.rl.core.record;

/**
 * Represents a column record.
 */
public class ColumnRecordModel {

    private String id;

    private String type;

    private String value;

    private boolean generated = false;

    /**
     * Creates an instance.
     * 
     * @param id    id of the column
     * @param type  type of the column
     * @param value value of the column in this record
     */
    public ColumnRecordModel(String id, String type, String value) {
        this.id = id;
        this.type = type;
        this.value = value;
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

    public void setGenerated(boolean generated) {
        this.generated = generated;
    }

    public boolean isGenerated() {
        return generated;
    }

    @Override
    public String toString() {
        return "" + id + "=\"" + value + "\"";
    }
}
