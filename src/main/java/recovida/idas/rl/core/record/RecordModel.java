package recovida.idas.rl.core.record;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a collection of column records.
 */
public class RecordModel {

    private Map<String, ColumnRecordModel> crm;

    public Collection<ColumnRecordModel> getColumnRecordModels() {
        return crm.values();
    }

    public ColumnRecordModel getColumnRecordModel(String id) {
        return crm.getOrDefault(id, null);
    }

    /**
     * Creates an instance.
     * 
     * @param columnRecordModels a collection of column records
     */
    public RecordModel(Collection<ColumnRecordModel> columnRecordModels) {
        this.crm = new LinkedHashMap<String, ColumnRecordModel>();
        for (ColumnRecordModel m : columnRecordModels)
            this.crm.put(m.getId(), m);
    }
}
