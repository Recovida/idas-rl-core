package recovida.idas.rl.core.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import recovida.idas.rl.core.util.StatusReporter;

public class ConfigReader {

    public static int MAX_NUMBER = 999;

    public ConfigModel readConfig(String propFileName) {
        if (propFileName == null)
            propFileName = "assets/config.properties";

        ConfigModel configModel = new ConfigModel();

        Properties config = new Properties();

        InputStream configFileStream;
        try {
            configFileStream = new FileInputStream(new File(propFileName));
        } catch (FileNotFoundException e1) {
            return null;
        }

        try {
            config.load(configFileStream);

            String[] mandatoryFields = { "db_a", "db_b", "db_index",
                    "linkage_folder" };
            List<String> missing = Arrays.stream(mandatoryFields)
                    .filter(f -> !config.containsKey(f))
                    .collect(Collectors.toList());
            if (!missing.isEmpty()) {
                missing.stream().forEach(f -> StatusReporter.get()
                        .errorMissingFieldInConfigFile(f));
                return null;
            }

            Path p = Paths.get(propFileName).getParent();

            configModel
                    .setDbA(p.resolve(config.getProperty("db_a")).toString());
            configModel
                    .setDbB(p.resolve(config.getProperty("db_b")).toString());
            configModel.setDbIndex(
                    p.resolve(config.getProperty("db_index")).toString());
            configModel.setLinkageDir(
                    p.resolve(config.getProperty("linkage_folder")).toString());

            configModel.setSuffixA(config.getProperty("suffix_a", "_dsa"));
            configModel.setSuffixB(config.getProperty("suffix_b", "_dsb"));
            if (config.containsKey("max_rows")) {
                String value = config.getProperty("max_rows");
                long l = -1;
                try {
                    l = Long.valueOf(value);
                } catch (NumberFormatException e) {
                }
                if (l < 0) {
                    StatusReporter.get().errorConfigFileHasInvalidValue(value,
                            "max_rows");
                    return null;
                }
                configModel.setMaxRows(l);
            }
            if (config.containsKey("num_threads")) {
                String value = config.getProperty("num_threads");
                int i = 0;
                try {
                    i = Integer.valueOf(value);
                } catch (NumberFormatException e) {
                }
                if (i < 0) {
                    StatusReporter.get().errorConfigFileHasInvalidValue(value,
                            "num_threads");
                    return null;
                }
                configModel.setThreadCount(i);
            }
            if (config.containsKey("min_score")) {
                String value = config.getProperty("min_score");
                float f = -1;
                try {
                    f = Float.valueOf(value) / 100;
                } catch (NumberFormatException e) {
                }
                if (f < 0 || f > 100) {
                    StatusReporter.get().errorConfigFileHasInvalidValue(value,
                            "min_score");
                    return null;
                }
                configModel.setMinimumScore(f);
            }
            if (config.containsKey("cleaning_regex")) {
                String value = config.getProperty("cleaning_regex");
                try {
                    Pattern.compile(value);
                    configModel.setCleaningRegex(value);
                } catch (PatternSyntaxException e) {
                    StatusReporter.get().errorConfigFileHasInvalidValue(value,
                            "cleaning_regex");
                    return null;
                }
            }
            if (config.containsKey("row_num_col_a"))
                configModel
                        .setRowNumColNameA(config.getProperty("row_num_col_a"));
            if (config.containsKey("row_num_col_b"))
                configModel
                        .setRowNumColNameB(config.getProperty("row_num_col_b"));
            if (config.containsKey("encoding_a"))
                configModel.setEncodingA(config.getProperty("encoding_a"));
            if (config.containsKey("encoding_b"))
                configModel.setEncodingB(config.getProperty("encoding_b"));

            if (config.containsKey("output_dec_sep"))
                configModel.setDecimalSeparator(config.getProperty("output_dec_sep"));
            if (config.containsKey("output_col_sep"))
                configModel.setColumnSeparator(config.getProperty("output_col_sep"));

            // row number
            configModel.addColumn(new ColumnConfigModel("_____NUM",
                    "numerical_id", configModel.getRowNumColNameA(),
                    configModel.getRowNumColNameB(),
                    configModel.getRowNumColNameA(),
                    configModel.getRowNumColNameB(), 0, 0));

            // read all columns
            for (int i = 0; i <= MAX_NUMBER; i++) {
                String id = "_COL" + i;
                String type = config.getProperty(i + "_type");
                String indexA = config.getProperty(i + "_index_a");
                String indexB = config.getProperty(i + "_index_b");

                if (type != null && type.equals("copy")) {
                    config.setProperty(i + "_weight", "0");
                    if (indexA == null && indexB != null)
                        indexA = "";
                    else if (indexB == null && indexA != null)
                        indexB = "";
                }

                String renameA = config.getProperty(i + "_rename_a",
                        indexA + "_" + configModel.getSuffixA());
                String renameB = config.getProperty(i + "_rename_b",
                        indexB + "_" + configModel.getSuffixB());

                // if any of the fields is missing, the column pair is thrown
                // away
                if (type == null || indexA == null || indexB == null
                        || config.getProperty(i + "_weight") == null) {
                    if (type != null || indexA != null || indexB != null)
                        StatusReporter.get().warnIgnoringColumn(i);
                    continue;
                }

                double weight = -1, phonWeight = 0;
                String value = config.getProperty(i + "_weight");
                try {
                    weight = Double.valueOf(value);
                } catch (NumberFormatException e) {
                }
                if (weight < 0) {
                    StatusReporter.get().errorConfigFileHasInvalidValue(value,
                            i + "_weight");
                    return null;
                }
                if ("name".equals(type)) {
                    value = config.getProperty(i + "_phon_weight",
                            "0.0");
                    try {
                        phonWeight = Double.valueOf(value);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                    if (phonWeight < 0) {
                        StatusReporter.get().errorConfigFileHasInvalidValue(
                                value, i + "_phon_weight");
                        return null;
                    }
                }

                ColumnConfigModel column = new ColumnConfigModel(id, type,
                        indexA, indexB, renameA, renameB, weight, phonWeight);
                configModel.addColumn(column);

                if (type.equals("name") && phonWeight > 0) {
                    ColumnConfigModel c = new ColumnConfigModel(id + "__PHON__",
                            "string", "", "", "", "", phonWeight, 0.0);
                    c.setGenerated(true);
                    configModel.addColumn(c);
                }
                configFileStream.close();
            }
        } catch (IOException e) {
            return null;
        }
        return configModel;
    }
}
