package recovida.idas.rl.core.record;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a pair of records and its score.
 */
public class RecordPairModel {

    private RecordModel recordA;

    private RecordModel recordB;

    private double score;

    private Map<String, Double> similarities;

    /**
     * Creates an instance from a pair of records and its score.
     * 
     * @param recordA the first record
     * @param recordB the second record
     * @param score   the score between {@code recordA} e {@code recordB}
     */
    public RecordPairModel(RecordModel recordA, RecordModel recordB,
            double score) {
        this.recordA = recordA;
        this.recordB = recordB;
        this.score = score;
        this.similarities = new LinkedHashMap<>();
    }

    public RecordModel getRecordA() {
        return recordA;
    }

    public void setRecordA(RecordModel recordA) {
        this.recordA = recordA;
    }

    public RecordModel getRecordB() {
        return recordB;
    }

    public void setRecordB(RecordModel recordB) {
        this.recordB = recordB;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public Double getSimilarity(String id) {
        return similarities.getOrDefault(id, null);
    }

    public void setSimilarity(String similarityCol, Double value) {
        similarities.put(similarityCol, value);
    }
}
