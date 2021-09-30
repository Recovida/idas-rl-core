package recovida.idas.rl.core.linkage;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import recovida.idas.rl.core.config.ColumnConfigModel;
import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.record.ColumnRecordModel;
import recovida.idas.rl.core.record.RecordPairModel;

public class LinkageOutput {

    private ConfigModel config;
    
    private Map<String, String> simColumns; // id -> similarity column name
    
    private char decSep;

    public LinkageOutput(ConfigModel config) {
        this.config = config;
        this.simColumns = new LinkedHashMap<>();
        this.decSep = config.getDecimalSeparator().getCharacter();
        String simColName;
        for (ColumnConfigModel c : config.getColumns()) {
            if ("name".equals(c.getType())
                    && (simColName = c.getSimilarityCol()) != null
                    && !simColName.isEmpty()) {
                simColumns.put(c.getId(), simColName);
            }
        }
    }

    private static String formatNumber(double number, char decSep) {
        String s = String.format(Locale.ENGLISH, "%.2f", number);
        if (decSep != '.')
            s = s.replace('.', decSep);
        return s;
    }

    public String fromRecordPairToCsv(RecordPairModel recordPair) {
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
        for (Entry<String, String> c : simColumns.entrySet()) {
            Double sim = recordPair.getSimilarity(c.getKey());
            if (sim != null)
                csvResult.append(formatNumber(100 * sim, decSep));
            csvResult.append(sep);
        }
        csvResult.append(formatNumber(100 * recordPair.getScore(), decSep));
        return csvResult.toString();
    }

    public String getCsvHeader() {
        StringBuilder headerResult = new StringBuilder();
        // for each column a add to result
        char sep = config.getColumnSeparator().getCharacter();
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated() || col.getType().equals("copy")
                    && col.getIndexA().equals(""))
                continue;
            headerResult.append(quote(col.getRenameA())).append(sep);
        }
        // for each column b add to result
        for (ColumnConfigModel col : config.getColumns()) {
            if (col.isGenerated() || col.getType().equals("copy")
                    && col.getIndexB().equals(""))
                continue;
            headerResult.append(quote(col.getRenameB())).append(sep);
        }
        // similarity
        for (String c : simColumns.values())
            headerResult.append(quote(c)).append(sep);
        headerResult.append(quote("score"));
        return headerResult.toString();
    }

    protected static String quote(String s) {
        if (s == null || s.isEmpty())
            return ""; // empty value does not need quotes
        return '"' + s.replace("\"", "\"\"") + '"';
    }
}
