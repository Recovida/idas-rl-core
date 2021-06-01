package recovida.idas.rl.core.linkage;

import java.util.Locale;

import recovida.idas.rl.core.config.ColumnConfigModel;
import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.record.ColumnRecordModel;
import recovida.idas.rl.core.record.RecordPairModel;

public class LinkageUtils {
    public String fromRecordPairToCsv(RecordPairModel recordPair) {
        StringBuilder csvResult = new StringBuilder();
        for (ColumnRecordModel column : recordPair.getRecordA()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            csvResult.append(quote(column.getValue())).append(";");
        }
        for (ColumnRecordModel column : recordPair.getRecordB()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            csvResult.append(quote(column.getValue())).append(";");
        }
        csvResult.append(String.format(Locale.ENGLISH, "%.2f",
                100 * recordPair.getScore()));
        return csvResult.toString();
    }

    public static String getCsvHeaderFromConfig(ConfigModel config) {
        StringBuilder headerResult = new StringBuilder();
        // for each column a add to result
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated() || (col.getType().equals("copy")
                    && col.getIndexA().equals("")))
                continue;
            headerResult.append(quote(col.getRenameA())).append(";");
        }
        // for each column b add to result
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated() || (col.getType().equals("copy")
                    && col.getIndexB().equals("")))
                continue;
            headerResult.append(quote(col.getRenameB())).append(";");
        }
        headerResult.append(quote("score"));
        return headerResult.toString();
    }

    protected static String quote(String s) {
        if (s == null)
            return ""; // empty value does not need quotes
        return '"' + s.replace("\"", "\"\"") + '"';
    }
}
