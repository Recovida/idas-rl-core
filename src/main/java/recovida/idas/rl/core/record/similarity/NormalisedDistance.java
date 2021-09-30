package recovida.idas.rl.core.record.similarity;

import org.apache.commons.text.similarity.FuzzyScore;
import org.apache.commons.text.similarity.SimilarityScore;

/**
 * Converts a {@link SimilarityScore} or a {@link FuzzyScore} into a normalised
 * version, in order to obtain a result in [0, 1].
 */
public class NormalisedDistance implements SimilarityScore<Double> {

    private SimilarityScore<Integer> absoluteDistance;

    private DenominatorSupplier denominatorSupplier;

    private String label;

    /**
     * This is an abstraction for a function that returns the number by which
     * the similarity of two strings will be divided. Must return a number
     * greater than zero and not smaller than the similarity between the
     * strings. The returned value will be used as a denominator so that the
     * score falls between 0 and 1.
     */
    public interface DenominatorSupplier {

        /**
         * Returns the number that will be used as a denominator to normalise
         * the similarity between the two given strings.
         * 
         * @param s1 the first string
         * @param s2 the second string
         * @return a number x such that x &gt; 0 and x &ge; the similarity of s1
         *         and s2
         */
        public int getDenominator(CharSequence s1, CharSequence s2);
    }

    /**
     * Creates an instance using a {@link SimilarityScore} as the absolute
     * distance.
     * 
     * @param absoluteDistance    the absolute distance to be normalised
     * @param denominatorSupplier the function that computes the denominator
     */
    public NormalisedDistance(SimilarityScore<Integer> absoluteDistance,
            DenominatorSupplier denominatorSupplier) {
        this.absoluteDistance = absoluteDistance;
        this.denominatorSupplier = denominatorSupplier;
        this.label = "Normalised" + absoluteDistance.getClass().getSimpleName();
    }

    /**
     * Creates an instance using a {@link FuzzyScore} as the absolute distance.
     * 
     * @param fs                  the absolute distance to be normalised
     * @param denominatorSupplier the function that computes the denominator
     */
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
