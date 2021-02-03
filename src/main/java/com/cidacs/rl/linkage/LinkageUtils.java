package com.cidacs.rl.linkage;

import com.cidacs.rl.config.ColumnConfigModel;
import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.record.ColumnRecordModel;
import com.cidacs.rl.record.RecordPairModel;

public class LinkageUtils {
    public String fromRecordPairToCsv(RecordPairModel recordPair){
        String csvResult = "";
        for(ColumnRecordModel column: recordPair.getRecordA().getColumnRecordModels()){
            if (column.isGenerated())
                continue;
            csvResult=csvResult+column.getOriginalValue()+",";
        }
        for(ColumnRecordModel column: recordPair.getRecordB().getColumnRecordModels()){
            if (column.isGenerated())
                continue;
            csvResult=csvResult+column.getOriginalValue()+",";
        }
        csvResult = csvResult + recordPair.getScore();
        return csvResult;
    }

    public String getCsvHeaderFromRecordPair(ConfigModel config, RecordPairModel recordPair) {
        String headerResult = "";
        for(ColumnRecordModel column: recordPair.getRecordA().getColumnRecordModels()){
            if (column.isGenerated())
                continue;
            headerResult=headerResult+column.getId() + config.getSuffixA() + ",";
        }
        for(ColumnRecordModel column: recordPair.getRecordB().getColumnRecordModels()){
            if (column.isGenerated())
                continue;
            headerResult=headerResult+column.getId() + config.getSuffixB() + ",";
        }
        headerResult = headerResult + "score";
        return headerResult;
    }

    public String getCsvHeaderFromConfig(ConfigModel config){
        String headerResult = "";
        // for each column a add to result
        for (ColumnConfigModel col: config.getColumns()){
            headerResult = headerResult + col.getIndexA()+"_dsa,";
        }
        // for each column b add to result
        for (ColumnConfigModel col: config.getColumns()){
            headerResult = headerResult + col.getIndexB()+"_dsb,";
        }
        headerResult = headerResult + "score";
        return headerResult;
    }
}
