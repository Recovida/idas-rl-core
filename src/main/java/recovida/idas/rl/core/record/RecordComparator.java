package recovida.idas.rl.core.record;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.spell.JaroWinklerDistance;

import recovida.idas.rl.core.config.ColumnConfigModel;
import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.util.StatusReporter;

/**
 * Provides an algorithm to compute the score of record pair.
 */
public class RecordComparator {

    private final ConfigModel config;

    /**
     * Creates an instance using a given configuration.
     * 
     * @param config the configuration
     */
    public RecordComparator(ConfigModel config) {
        this.config = config;
    }

    private double compareTwoRecords(RecordModel recordA, RecordModel recordB) {
        double tmpTotal = 0.0;// 2 + (0.125 * 8) + 0.08 + 0.04 + 0.5;
        double score;
        double penalty = 0.0;

        ColumnRecordModel columnA = null;
        ColumnRecordModel columnB = null;

        double scoreNomes = 0.0, scoreDates = 0.0, scoreIbge = 0.0,
                scoreCategorical = 0.0, scoreGender = 0.0,
                scoreNumericalId = 0.0;

        for (ColumnConfigModel columnConfig : config.getColumns()) {
            // nao avaliar colunas sem peso
            if (columnConfig.getWeight() == 0.0) {
                continue;
            }

            // select columA
            for (ColumnRecordModel tmpColumnA : recordA
                    .getColumnRecordModels()) {
                if (columnConfig.getId().equals(tmpColumnA.getId())) {
                    columnA = tmpColumnA;
                }
            }

            // select columB
            for (ColumnRecordModel tmpColumnB : recordB
                    .getColumnRecordModels()) {
                if (columnConfig.getId().equals(tmpColumnB.getId())) {
                    columnB = tmpColumnB;
                }
            }

            // PARA NOME E NOME DA MAE
            if (columnConfig.getType().equals("name")
                    || columnConfig.getType().equals("string")) {
                if (columnA.getValue().isEmpty() == false
                        && columnB.getValue().isEmpty() == false) {
                    tmpTotal = tmpTotal + columnConfig.getWeight();
                    scoreNomes = scoreNomes + getDistanceString(
                            columnA.getValue(), columnB.getValue(),
                            columnConfig.getWeight());
                } else {
                    penalty = penalty + 0.02;
                }
            }
            // PARA DATA DE NASCIMENTO
            else if (columnConfig.getType().equals("date")) {
                try {
                    if (columnA.getValue().isEmpty() == false
                            && columnB.getValue().isEmpty() == false) {
                        tmpTotal = tmpTotal + columnConfig.getWeight();
                        scoreDates = scoreDates + getDistanceDate(
                                columnA.getValue(), columnB.getValue(),
                                columnConfig.getWeight());
                    } else {
                        penalty = penalty + 0.01;
                    }
                } catch (ArrayIndexOutOfBoundsException dta) {
                    StatusReporter.get().warnInvalidValueForType(
                            columnA.getValue(), "date");
                }
            }
            // PARA CODIGO DO MUNIC
            else if (columnConfig.getType().equals("ibge")) {
                try {
                    if ("".equals(columnA.getValue()) == false
                            && "".equals(columnB.getValue()) == false) {
                        if (columnA.getValue().length() == 6
                                && columnB.getValue().length() == 6) {
                            tmpTotal = tmpTotal + columnConfig.getWeight();
                            scoreIbge = scoreIbge + getDistanceIBGE(
                                    columnA.getValue(), columnB.getValue(),
                                    columnConfig.getWeight());
                        }
                    }
                } catch (StringIndexOutOfBoundsException ibge) {
                    StatusReporter.get().warnInvalidValueForType(
                            columnA.getValue(), "ibge");
                }
            }
            // PARA SEXO
            else if (columnConfig.getType().equals("gender")) {
                try {
                    if (columnA.getValue().isEmpty() == false
                            && columnB.getValue().isEmpty() == false) {
                        tmpTotal = tmpTotal + columnConfig.getWeight();
                        if (columnA.getValue()
                                .charAt(columnA.getValue().length()
                                        - 1) == columnB.getValue().charAt(
                                                columnB.getValue().length()
                                                        - 1)) {
                            scoreGender = scoreGender
                                    + columnConfig.getWeight();
                        } else {
                            scoreGender = scoreGender + getDistanceCategorical(
                                    columnA.getValue(), columnB.getValue(),
                                    columnConfig.getWeight());
                        }
                    }
                } catch (StringIndexOutOfBoundsException ibge) {
                    StatusReporter.get().warnInvalidValueForType(
                            columnA.getValue(), "gender");
                }
            }

            // PARA CATEGORICAS
            else if (columnConfig.getType().equals("categorical")) {
                try {
                    if (columnA.getValue().isEmpty() == false
                            && columnB.getValue().isEmpty() == false) {
                        tmpTotal = tmpTotal + columnConfig.getWeight();
                        scoreCategorical = scoreCategorical
                                + getDistanceCategorical(columnA.getValue(),
                                        columnB.getValue(),
                                        columnConfig.getWeight());
                    }
                } catch (StringIndexOutOfBoundsException ibge) {
                    StatusReporter.get().warnInvalidValueForType(
                            columnA.getValue(), "categorical");
                }
            }

            else if (columnConfig.getType().equals("numerical_id")) {
                try {
                    if (columnA.getValue().isEmpty() == false
                            && columnB.getValue().isEmpty() == false) {
                        tmpTotal = tmpTotal + columnConfig.getWeight();
                        scoreNumericalId = scoreNumericalId
                                + getDistanceNumericalId(columnA.getValue(),
                                        columnB.getValue(),
                                        columnConfig.getWeight());
                    }
                } catch (StringIndexOutOfBoundsException e) {
                    StatusReporter.get().warnInvalidValueForType(
                            columnA.getValue(), "numerical_id");
                }
            }
        }
        if (penalty >= 0.03) {
            penalty = penalty * 2;
        }

        score = scoreCategorical + scoreDates + scoreIbge + scoreNomes
                + scoreGender + scoreNumericalId;
        return (score / tmpTotal) - penalty;
    }

    // CHECKSTYLE.OFF: JavadocMethod

    public RecordPairModel findBestCandidatePair(RecordModel record,
            ArrayList<RecordModel> candidates) {
        RecordModel tmpBestCandidate = null;
        double tmpScore, maxScore = 0;

        for (RecordModel candidate : candidates) {
            tmpScore = compareTwoRecords(record, candidate);
            if (tmpScore > maxScore) {
                maxScore = tmpScore;
                tmpBestCandidate = candidate;
            }

        }
        if (tmpBestCandidate != null) {
            return new RecordPairModel(record, tmpBestCandidate, maxScore);
        }
        return null;

    }

    private double getDistanceNumericalId(String id1, String id2, double w) {
        id1 = id1.replaceAll("[^0-9]", "");
        id2 = id2.replaceAll("[^0-9]", "");
        int length = Math.max(id1.length(), id2.length());
        id1 = StringUtils.leftPad(id1, length, '0');
        id2 = StringUtils.leftPad(id2, length, '0');
        return getDistanceString(id1, id2, w);
    }

    private double getDistanceString(String nome1, String nome2, double w) {
        JaroWinklerDistance jaro = new JaroWinklerDistance();
        return w * jaro.getDistance(nome1, nome2);
    }

    private double getDistanceDate(String data1, String data2, double w) {
        double score = 0;
        data1 = data1.replaceAll("-|/", "");
        data2 = data2.replaceAll("-|/", "");

        int minLength = Math.min(data1.length(), data2.length());

        for (int i = 0; i < minLength; i++) {
            if (data1.charAt(i) == data2.charAt(i)) {
                score = score + w / minLength;
            }
        }
        return score;
    }

    private double getDistanceIBGE(String ibge1, String ibge2, double w) {
        double score = 0;

        if (ibge1.substring(0, 2).equals(ibge2.substring(0, 2))) {
            // one third
            score = score + w / 3;
            if (ibge1.substring(2, 6).equals(ibge2.substring(2, 6))) {
                // two third
                score = score + (w / 3) * 2;
            }
        }
        return score;
    }

    private double getDistanceCategorical(String literal1, String literal2,
            double w) {
        if (literal1.equals(literal2)) {
            return w;
        }
        return 0.0;
    }
}
