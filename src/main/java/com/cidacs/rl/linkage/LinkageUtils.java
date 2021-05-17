package com.cidacs.rl.linkage;

import java.util.Locale;

import com.cidacs.rl.config.ColumnConfigModel;
import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.record.ColumnRecordModel;
import com.cidacs.rl.record.RecordPairModel;

public class LinkageUtils {
    public String fromRecordPairToCsv(RecordPairModel recordPair) {
        String csvResult = "";
        for (ColumnRecordModel column : recordPair.getRecordA()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            csvResult = csvResult + quote(column.getValue()) + ";";
        }
        for (ColumnRecordModel column : recordPair.getRecordB()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            csvResult = csvResult + quote(column.getValue()) + ";";
        }
        csvResult = csvResult + String.format(Locale.ENGLISH, "%.2f",
                100 * recordPair.getScore());
        return csvResult;
    }

    public static String getCsvHeaderFromConfig(ConfigModel config) {
        String headerResult = "";
        // for each column a add to result
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated() || (col.getType().equals("copy") && col.getIndexA().equals("")))
                continue;
            headerResult = headerResult + quote(col.getRenameA()) + ";";
        }
        // for each column b add to result
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated() || (col.getType().equals("copy") && col.getIndexB().equals("")))
                continue;
            headerResult = headerResult + quote(col.getRenameB()) + ";";
        }
        headerResult = headerResult + quote("score");
        return headerResult;
    }

    protected static String quote(String s) {
        if (s == null)
            return ""; // empty value does not need quotes
        return '"' + s.replaceAll("\"", "\"\"") + '"';
    }
}
