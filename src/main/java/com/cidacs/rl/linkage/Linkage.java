package com.cidacs.rl.linkage;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import com.cidacs.rl.config.ColumnConfigModel;
import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.record.ColumnRecordModel;
import com.cidacs.rl.record.RecordModel;
import com.cidacs.rl.record.RecordPairModel;
import com.cidacs.rl.search.Searching;

public class Linkage implements Serializable {
    private static final long serialVersionUID = 1L;
    private ConfigModel config;

    public Linkage(ConfigModel config) {
        this.config = config;
    }

    public String linkSpark(RecordModel record) {
        Searching searching = new Searching(this.config);
        LinkageUtils linkageUtils = new LinkageUtils();
        RecordPairModel candidatePair = searching.getCandidatePairFromRecord(record);
        if (candidatePair != null) {
            return linkageUtils.fromRecordPairToCsv(candidatePair);
        } else {
            Logger.getLogger(getClass()).warn("Could not link row.");
            Logger.getLogger(getClass()).debug("This is the row that could not be linked: " + record.getColumnRecordModels());
            return "";
        }
    }

    public RecordModel fromCSVRecordToRecord(CSVRecord csvRecord){
        ColumnRecordModel tmpRecordColumnRecord;
        String tmpIndex;
        String tmpValue;
        String tmpId;
        String tmpType;
        ArrayList<ColumnRecordModel> tmpRecordColumns;

        tmpRecordColumns = new ArrayList<>();
        for(ColumnConfigModel column : config.getColumns()){
            tmpIndex = column.getIndexB();
            String originalValue = csvRecord.get(tmpIndex);
            tmpValue = originalValue.replaceAll("[^A-Z0-9 ]", "").replaceAll("\\s+", " ").trim();
            tmpId = column.getId();
            tmpType = column.getType();
            tmpRecordColumnRecord = new ColumnRecordModel(tmpId, tmpType, tmpValue, originalValue);
            tmpRecordColumns.add(tmpRecordColumnRecord);
        }
        RecordModel recordModel = new RecordModel(tmpRecordColumns);
        return recordModel;
    }
}
