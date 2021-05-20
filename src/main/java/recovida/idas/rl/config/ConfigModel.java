package recovida.idas.rl.config;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

public class ConfigModel implements Serializable {
    private static final long serialVersionUID = 1L;
    private String dbA;
    private String dbB;
    private String encodingA = "UTF-8";
    private String encodingB = "UTF-8";
    private String suffixA = "_dsa";
    private String suffixB = "_dsb";
    private String rowNumColNameA = "#A";
    private String rowNumColNameB = "#B";
    private String dbIndex;
    private String linkageDir = ".";
    private long maxRows = Long.MAX_VALUE;
    private float minimumScore = 0;

    private ArrayList<ColumnConfigModel> columns = new ArrayList<>();

    public ConfigModel() {

    }

    public ConfigModel(String dbA, String dbB, String dbIndex, ArrayList<ColumnConfigModel> columns) {
        this.setDbA(dbA);
        this.setDbB(dbB);
        this.setDbIndex(dbIndex);
        this.columns = columns;
        this.maxRows = Long.MAX_VALUE;
        this.minimumScore = 0;
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
        this.columns.add(column);
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


}
