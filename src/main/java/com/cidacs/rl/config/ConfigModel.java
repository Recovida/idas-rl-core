package com.cidacs.rl.config;

import java.io.Serializable;
import java.util.ArrayList;

public class ConfigModel implements Serializable {
    private static final long serialVersionUID = 1L;
    private String dbA;
    private String dbB;
    private String suffixA = "_dsa";
    private String suffixB = "_dsb";
    private String dbIndex;
    private String linkageDir = ".";
    private long maxRows;

    private ArrayList<ColumnConfigModel> columns = new ArrayList<>();

    public ConfigModel() {

    }

    public ConfigModel(String dbA, String dbB, String dbIndex, ArrayList<ColumnConfigModel> columns) {
        this.dbA = dbA;
        this.dbB = dbB;
        this.dbIndex = dbIndex;
        this.columns = columns;
        this.maxRows = Long.MAX_VALUE;
    }

    public String getDbA() {
        return dbA;
    }

    public void setDbA(String dbA) {
        this.dbA = dbA;
    }

    public String getDbB() {
        return dbB;
    }

    public void setDbB(String dbB) {
        this.dbB = dbB;
    }

    public String getDbIndex() {
        return dbIndex;
    }

    public void setDbIndex(String dbIndex) {
        this.dbIndex = dbIndex;
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


}
