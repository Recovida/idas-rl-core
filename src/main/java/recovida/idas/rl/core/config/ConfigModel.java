package recovida.idas.rl.core.config;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import recovida.idas.rl.core.io.Separator;

public class ConfigModel implements Serializable {
    private static final long serialVersionUID = 1L;
    private String dbA;
    private String dbB;
    private String encodingA = "UTF-8";
    private String encodingB = "UTF-8";
    private String suffixA;
    private String suffixB;
    private String cleaningRegex = "";
    private String rowNumColNameA = "#A";
    private String rowNumColNameB = "#B";
    private String dbIndex;
    private String linkageDir = ".";
    private Separator decimalSeparator = Separator.DEFAULT_DEC_SEP;
    private Separator columnSeparator = Separator.DEFAULT_COL_SEP;
    private long maxRows = Long.MAX_VALUE;
    private float minimumScore = 0;
    private int threadCount = Math.min(8,
            Runtime.getRuntime().availableProcessors());

    private ArrayList<ColumnConfigModel> columns = new ArrayList<>();

    public ConfigModel() {

    }

    public ConfigModel(String dbA, String dbB, String dbIndex,
            ArrayList<ColumnConfigModel> columns) {
        setDbA(dbA);
        setDbB(dbB);
        setDbIndex(dbIndex);
        this.columns = columns;
        maxRows = Long.MAX_VALUE;
        minimumScore = 0;
    }

    public String getDbA() {
        return dbA;
    }

    public void setDbA(String dbA) {
        this.dbA = new File(dbA).getPath();
    }

    public String getDbB() {
        return dbB;
    }

    public void setDbB(String dbB) {
        this.dbB = new File(dbB).getPath();
    }

    public String getDbIndex() {
        return dbIndex;
    }

    public void setDbIndex(String dbIndex) {
        this.dbIndex = new File(dbIndex).getPath();
    }

    public String getSuffixA() {
        return suffixA;
    }

    public void setSuffixA(String suffixA) {
        this.suffixA = suffixA;
    }

    public String getSuffixB() {
        return suffixB;
    }

    public void setSuffixB(String suffixB) {
        this.suffixB = suffixB;
    }

    public ArrayList<ColumnConfigModel> getColumns() {
        return columns;
    }

    public void addColumn(ColumnConfigModel column) {
        columns.add(column);
    }

    public void setColumns(ArrayList<ColumnConfigModel> columns) {
        this.columns = columns;
    }

    public long getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(long maxRows) {
        this.maxRows = maxRows;
    }

    public String getLinkageDir() {
        return linkageDir;
    }

    public void setLinkageDir(String linkageDir) {
        this.linkageDir = linkageDir;
    }

    public float getMinimumScore() {
        return minimumScore;
    }

    public void setMinimumScore(float minimumScore) {
        this.minimumScore = minimumScore;
    }

    public String getRowNumColNameA() {
        return rowNumColNameA;
    }

    public void setRowNumColNameA(String rowNumColNameA) {
        this.rowNumColNameA = rowNumColNameA;
    }

    public String getRowNumColNameB() {
        return rowNumColNameB;
    }

    public void setRowNumColNameB(String rowNumColNameB) {
        this.rowNumColNameB = rowNumColNameB;
    }

    public String getEncodingA() {
        return encodingA;
    }

    public void setEncodingA(String encodingA) {
        this.encodingA = convertNonStandardEncodingName(encodingA);
    }

    public String getEncodingB() {
        return encodingB;
    }

    public void setEncodingB(String encodingB) {
        this.encodingB = convertNonStandardEncodingName(encodingB);
    }

    protected static String convertNonStandardEncodingName(String encoding) {
        String enc = encoding.toUpperCase().replaceAll("[^A-Z0-9]", "");
        if (enc.equals("ANSI"))
            return "Cp1252";
        return encoding;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        if (threadCount > 0)
            this.threadCount = threadCount;
    }

    public String getCleaningRegex() {
        return cleaningRegex;
    }

    public void setCleaningRegex(String regex) {
        if (regex == null)
            return;
        try {
            Pattern.compile(regex);
            cleaningRegex = regex;
        } catch (PatternSyntaxException e) {
            cleaningRegex = "";
        }
    }

    public Separator getDecimalSeparator() {
        return decimalSeparator;
    }

    public void setDecimalSeparator(Separator decimalSeparator) {
        if (decimalSeparator != null)
            this.decimalSeparator = decimalSeparator;
    }

    public void setDecimalSeparator(String decimalSeparator) {
        setDecimalSeparator(Separator.fromName(decimalSeparator));
    }

    public Separator getColumnSeparator() {
        return columnSeparator;
    }

    public void setColumnSeparator(Separator columnSeparator) {
        if (columnSeparator != null)
            this.columnSeparator = columnSeparator;
    }

    public void setColumnSeparator(String columnSeparator) {
        setColumnSeparator(Separator.fromName(columnSeparator));
    }

}
