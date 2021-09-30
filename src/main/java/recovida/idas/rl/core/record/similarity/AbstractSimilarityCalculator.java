package recovida.idas.rl.core.record.similarity;

import org.apache.commons.text.similarity.SimilarityScore;

/**
 * An abstraction for algorithms that compute the similarity between a pair of
 * strings.
 */
public abstract class AbstractSimilarityCalculator {

    /**
     * Computes the similarity between two strings.
     * 
     * @param s1 the first string
     * @param s2 the second string
     * @return the similarity between {@code s1} and {@code s2}
     */
    public abstract double compute(String s1, String s2);

    /**
     * Creates a {@link AbstractSimilarityCalculator} from an Apache
     * {@link SimilarityScore} object.
     * 
     * @param <T> the numeric type of the returned value
     * @param ss  the {@link SimilarityScore} instance
     * @return the similarity computed by {@code ss}
     */
    public static <T extends Number> AbstractSimilarityCalculator fromSimilarityScore(
            SimilarityScore<T> ss) {
        return new AbstractSimilarityCalculator() {

            @Override
            public double compute(String s1, String s2) {
                return ss.apply(s1, s2).doubleValue();
            }

            @Override
            public String toString() {
                return ss instanceof NormalisedDistance ? ss.toString()
                        : ss.getClass().getSimpleName();
            }
        };
    }

    /**
     * Creates a {@link AbstractSimilarityCalculator} from an Apache
     * {@link SimilarityScore} object and reverts the result (x -> 1 - x).
     * 
     * @param <T> the numeric type of the returned value
     * @param ss  the {@link SimilarityScore} instance
     * @return the complement of the similarity computed by {@code ss}
     */
    public static <T extends Number> AbstractSimilarityCalculator fromComplementarySimilarityScore(
            SimilarityScore<T> ss) {
        return new AbstractSimilarityCalculator() {

            @Override
            public double compute(String s1, String s2) {
                return 1 - ss.apply(s1, s2).doubleValue();
            }

            @Override
            public String toString() {
                return "Complementary"
                        + (ss instanceof NormalisedDistance ? ss.toString()
                                : ss.getClass().getSimpleName());
            }
        };
    }
}
