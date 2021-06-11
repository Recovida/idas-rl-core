package recovida.idas.rl.core.linkage;

import java.util.Locale;

import recovida.idas.rl.core.config.ColumnConfigModel;
import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.record.ColumnRecordModel;
import recovida.idas.rl.core.record.RecordPairModel;

public class LinkageUtils {
    public static String fromRecordPairToCsv(ConfigModel config,
            RecordPairModel recordPair) {
        StringBuilder csvResult = new StringBuilder();
        char sep = config.getColumnSeparator().getCharacter();
        for (ColumnRecordModel column : recordPair.getRecordA()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            csvResult.append(quote(column.getValue())).append(sep);
        }
        for (ColumnRecordModel column : recordPair.getRecordB()
                .getColumnRecordModels()) {
            if (column.isGenerated())
                continue;
            csvResult.append(quote(column.getValue())).append(sep);
        }
        String s = String.format(Locale.ENGLISH, "%.2f",
                100 * recordPair.getScore());
        char decSep = config.getDecimalSeparator().getCharacter();
        if (decSep != '.')
            s = s.replace('.', decSep);
        csvResult.append(s);
        return csvResult.toString();
    }

    public static String getCsvHeaderFromConfig(ConfigModel config) {
        StringBuilder headerResult = new StringBuilder();
        // for each column a add to result
        char sep = config.getColumnSeparator().getCharacter();
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated() || (col.getType().equals("copy")
                    && col.getIndexA().equals("")))
                continue;
            headerResult.append(quote(col.getRenameA())).append(sep);
        }
        // for each column b add to result
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated() || (col.getType().equals("copy")
                    && col.getIndexB().equals("")))
                continue;
            headerResult.append(quote(col.getRenameB())).append(sep);
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
