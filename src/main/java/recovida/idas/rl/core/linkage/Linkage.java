package recovida.idas.rl.core.linkage;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.text.similarity.LongestCommonSubsequenceDistance;

import recovida.idas.rl.core.config.ColumnConfigModel;
import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.record.RecordModel;
import recovida.idas.rl.core.record.RecordPairModel;
import recovida.idas.rl.core.record.similarity.NormalisedDistance;
import recovida.idas.rl.core.record.similarity.AbstractSimilarityCalculator;
import recovida.idas.rl.core.search.Searching;

/**
 * Provides a method to perform record linkage.
 */
public class Linkage implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private final ConfigModel config;

    private Searching searching;

    private static final AbstractSimilarityCalculator SIMILARITY_CALCULATOR = AbstractSimilarityCalculator
            .fromComplementarySimilarityScore(new NormalisedDistance(
                    new LongestCommonSubsequenceDistance(),
                    (left, right) -> Math.max(left.length() + right.length(),
                            1)));

    /**
     * Creates an instance from a configuration.
     * 
     * @param config linkage configuration
     */
    public Linkage(ConfigModel config) {
        this.config = config;
        try {
            searching = new Searching(config);
        } catch (IOException e) {
            searching = null;
        }
    }

    /**
     * Attempts to find a correspondence (on dataset B) to a given record (from
     * dataset A).
     * 
     * @param record record from dataset A
     * @return a good correspondence in dataset B, or {@code null} if it cannot
     *         be found
     */
    public RecordPairModel link(RecordModel record) {
        if (searching == null || record == null)
            return null;
        RecordPairModel candidatePair = searching
                .getCandidatePairFromRecord(record);
        if (candidatePair == null)
            return null;
        if (candidatePair.getScore() >= config.getMinimumScore()) {
            String simCol;
            for (ColumnConfigModel c : config.getColumns()) {
                if ("name".equals(c.getType())
                        && (simCol = c.getSimilarityCol()) != null
                        && !simCol.isEmpty()) {
                    String a = candidatePair.getRecordA()
                            .getColumnRecordModel(c.getId()).getValue();
                    String b = candidatePair.getRecordB()
                            .getColumnRecordModel(c.getId()).getValue();
                    if (a != null && b != null && !a.isEmpty()
                            && !b.isEmpty()) {
                        double similarity = SIMILARITY_CALCULATOR.compute(a, b);
                        if (similarity < c.getSimilarityMin())
                            return null;
                        candidatePair.setSimilarity(c.getId(), similarity);
                    }
                }
            }
            return candidatePair;
        }
        return null;
    }

    @Override
    public void close() {
        if (searching != null) {
            searching.close();
        }
    }

    @Override
    public void finalize() {
        close();
    }

}
