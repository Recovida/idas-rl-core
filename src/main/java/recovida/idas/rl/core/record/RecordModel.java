package recovida.idas.rl.core.record;

import java.util.ArrayList;

public class RecordModel {

    private ArrayList<ColumnRecordModel> columnRecordModels;

    public ArrayList<ColumnRecordModel> getColumnRecordModels() {
        return columnRecordModels;
    }

    public void setColumnRecordModels(
            ArrayList<ColumnRecordModel> columnRecordModels) {
        this.columnRecordModels = columnRecordModels;
    }

    public RecordModel(ArrayList<ColumnRecordModel> columnRecordModels) {

        this.columnRecordModels = columnRecordModels;
    }

    public RecordModel() {

    }
}
