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
import recovida.idas.rl.core.record.similarity.SimilarityCalculator;
import recovida.idas.rl.core.search.Searching;

public class Linkage implements Serializable, Closeable {
    private static final long serialVersionUID = 1L;
    private final ConfigModel config;
    private Searching searching;
    private final static SimilarityCalculator similarityCalculator = SimilarityCalculator
            .fromComplementarySimilarityScore(new NormalisedDistance(
                    new LongestCommonSubsequenceDistance(),
                    (left, right) -> Math.max(left.length() + right.length(),
                            1)));

    public Linkage(ConfigModel config) {
        this.config = config;
        try {
            searching = new Searching(config);
        } catch (IOException e) {
            searching = null;
        }
    }

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
                    String a = candidatePair.getRecordA().getColumnRecordModel(c.getId()).getValue();
                    String b = candidatePair.getRecordB().getColumnRecordModel(c.getId()).getValue();
                    if (a != null && b != null && !a.isEmpty() && !b.isEmpty()) {
                        double similarity = similarityCalculator.compute(a, b);
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
