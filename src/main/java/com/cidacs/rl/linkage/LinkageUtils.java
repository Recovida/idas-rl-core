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
            csvResult = csvResult + quote(column.getOriginalValue()) + ";";
        }
        for (ColumnRecordModel column : recordPair.getRecordB()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            csvResult = csvResult + quote(column.getOriginalValue()) + ";";
        }
        csvResult = csvResult + String.format(Locale.ENGLISH, "%.2f",
                100 * recordPair.getScore());
        return csvResult;
    }

    public String getCsvHeaderFromRecordPair(ConfigModel config,
            RecordPairModel recordPair) {
        String headerResult = "";
        for (ColumnRecordModel column : recordPair.getRecordA()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            headerResult = headerResult
                    + quote(column.getId() + "_" + config.getSuffixA()) + ";";
        }
        for (ColumnRecordModel column : recordPair.getRecordB()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            headerResult = headerResult
                    + quote(column.getId() + "_" + config.getSuffixB()) + ";";
        }
        headerResult = headerResult + quote("score");
        return headerResult;
    }

    public static String getCsvHeaderFromConfig(ConfigModel config) {
        String headerResult = "";
        // for each column a add to result
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated())
                continue;
            headerResult = headerResult
                    + quote(col.getIndexA() + "_" + config.getSuffixA()) + ";";
        }
        // for each column b add to result
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated())
                continue;
            headerResult = headerResult
                    + quote(col.getIndexB() + "_" + config.getSuffixB()) + ";";
        }
        headerResult = headerResult + quote("score");
        return headerResult;
    }

    protected static String quote(String s) {
        return '"' + s.replaceAll("\"", "\"\"") + '"';
    }
}
