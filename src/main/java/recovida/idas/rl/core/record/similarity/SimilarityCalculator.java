package recovida.idas.rl.core.record.similarity;

import org.apache.commons.text.similarity.SimilarityScore;

public abstract class SimilarityCalculator {

    public abstract double compute(String s1, String s2);

    public static <T extends Number> SimilarityCalculator fromSimilarityScore(
            SimilarityScore<T> ss) {
        return new SimilarityCalculator() {

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

    public static <T extends Number> SimilarityCalculator fromComplementarySimilarityScore(
            SimilarityScore<T> ss) {
        return new SimilarityCalculator() {

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
