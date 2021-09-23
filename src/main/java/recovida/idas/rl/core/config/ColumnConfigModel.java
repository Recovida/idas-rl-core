package recovida.idas.rl.core.config;

import java.io.Serializable;

public class ColumnConfigModel implements Serializable {
    private static final long serialVersionUID = 1L;
    private String id;
    private String type;
    private String indexA;
    private String indexB;
    private String renameA;
    private String renameB;
    private double weight;
    private double phonWeight;
    private boolean generated;
    private String similarityCol = "";
    private double similarityMin = 0.0;

    public ColumnConfigModel(String id, String type, String indexA,
            String indexB, String renameA, String renameB, double weight) {
        this(id, type, indexA, indexB, renameA, renameB, weight, 0.0);
    }

    public ColumnConfigModel(String id, String type, String indexA,
            String indexB, String renameA, String renameB, double weight,
            double phonWeight) {
        this(id, type, indexA, indexB, renameA, renameB, weight, phonWeight,
                null, 0.0);
    }

    public ColumnConfigModel(String id, String type, String indexA,
            String indexB, String renameA, String renameB, double weight,
            double phonWeight, String similarityCol, double similarityMin) {
        this.id = id;
        this.type = type;
        this.indexA = indexA;
        this.indexB = indexB;
        this.renameA = renameA;
        this.renameB = renameB;
        this.weight = weight;
        this.phonWeight = phonWeight;
        this.similarityCol = similarityCol;
        this.similarityMin = similarityMin;
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

    public String getRenameA() {
        return renameA;
    }

    public void setRenameA(String renameA) {
        this.renameA = renameA;
    }

    public String getRenameB() {
        return renameB;
    }

    public void setRenameB(String renameB) {
        this.renameB = renameB;
    }

    public String getSimilarityCol() {
        return similarityCol;
    }

    public void setSimilarityCol(String similarityCol) {
        this.similarityCol = similarityCol;
    }

    public double getSimilarityMin() {
        return similarityMin;
    }

    public void setSimilarityMin(double similarityMin) {
        this.similarityMin = similarityMin;
    }

}
