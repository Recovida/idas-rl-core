package recovida.idas.rl.core.record.similarity;

import org.apache.commons.text.similarity.FuzzyScore;
import org.apache.commons.text.similarity.SimilarityScore;

public class NormalisedDistance implements SimilarityScore<Double> {

    private SimilarityScore<Integer> absoluteDistance;
    private DenominatorSupplier denominatorSupplier;
    private String label;

    public interface DenominatorSupplier {
        public int getDenominator(CharSequence s1, CharSequence s2);
    }

    public NormalisedDistance(SimilarityScore<Integer> absoluteDistance,
            DenominatorSupplier denominatorSupplier) {
        this.absoluteDistance = absoluteDistance;
        this.denominatorSupplier = denominatorSupplier;
        this.label = "Normalised" + absoluteDistance.getClass().getSimpleName();
    }
    
    public NormalisedDistance(FuzzyScore fs,
            DenominatorSupplier denominatorSupplier) {
        this.absoluteDistance = new SimilarityScore<Integer>() {

            @Override
            public Integer apply(CharSequence left, CharSequence right) {
                return fs.fuzzyScore(left, right);
            }
        };
        this.denominatorSupplier = denominatorSupplier;
        this.label = "Normalised" + fs.getClass().getSimpleName();
    }

    @Override
    public Double apply(CharSequence left, CharSequence right) {
        return (double) absoluteDistance.apply(left, right)
                / denominatorSupplier.getDenominator(left, right);
    }
    
    @Override
    public String toString() { 
        return label;
    }

}
