package com.cidacs.rl.config;

public class ColumnConfigModel {
    private String id;
    private String type;
    private String indexA;
    private String indexB;
    private double weight;
    private double phonWeight;
    private boolean generated;

    public ColumnConfigModel(String id, String type, String indexA, String indexB, double weight) {
        this(id, type, indexA, indexB, weight, 0.0);
    }

    public ColumnConfigModel(String id, String type, String indexA, String indexB, double weight, double phonWeight) {
        this.id = id;
        this.type = type;
        this.indexA = indexA;
        this.indexB = indexB;
        this.weight = weight;
        this.phonWeight = phonWeight;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIndexA() {
        return indexA;
    }

    public void setIndedA(String indexA) {
        this.indexA = indexA;
    }

    public String getIndexB() {
        return indexB;
    }

    public void setIndexB(String indexB) {
        this.indexB = indexB;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public double getPhonWeight() {
        return phonWeight;
    }

    public void setPhonWeight(double phonWeight) {
        this.phonWeight = phonWeight;
    }

    public void setGenerated(boolean generated) {
        this.generated = generated;
    }

    public boolean isGenerated() {
        return generated;
    }
}
