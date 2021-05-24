package recovida.idas.rl.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import recovida.idas.rl.util.StatusReporter;

public class ConfigReader {

    public ConfigModel readConfig(String propFileName) {
        if (propFileName == null)
            propFileName = "assets/config.properties";

        ConfigModel configModel = new ConfigModel();

        Properties config = new Properties();

        InputStream configFileStream;
        try {
            configFileStream = new FileInputStream(new File(propFileName));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            return configModel;
        }

        try {
            config.load(configFileStream);

            // read dba, dbb, dbIndex and others
            configModel.setDbA(config.getProperty("db_a"));
            configModel.setDbB(config.getProperty("db_b"));
            configModel.setSuffixA(config.getProperty("suffix_a"));
            configModel.setSuffixB(config.getProperty("suffix_b"));
            configModel.setDbIndex(config.getProperty("db_index"));
            configModel.setLinkageDir(config.getProperty("linkage_folder"));
            if (config.containsKey("max_rows"))
                configModel.setMaxRows(
                        Long.valueOf(config.getProperty("max_rows")));
            if (config.containsKey("num_threads"))
                configModel.setThreadCount(
                        Integer.valueOf(config.getProperty("num_threads")));
            if (config.containsKey("min_score"))
                configModel.setMinimumScore(
                        Float.valueOf(config.getProperty("min_score")) / 100);
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

            // row number
            configModel.addColumn(new ColumnConfigModel("_____NUM",
                    "numerical_id", configModel.getRowNumColNameA(),
                    configModel.getRowNumColNameB(),
                    configModel.getRowNumColNameA(),
                    configModel.getRowNumColNameB(), 0, 0));

            // read all columns
            for (int i = 0; i <= 999; i++) {
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

                // if any of the columns is missing the columns is thrown away
                if (type == null || indexA == null || indexB == null
                        || config.getProperty(i + "_weight") == null) {
                    if (type != null || indexA != null || indexB != null)
                        StatusReporter.get().warnIgnoringColumn(i);
                    continue;
                }

                double weight = Double
                        .valueOf(config.getProperty(i + "_weight"));
                double phonWeight = Double
                        .valueOf(config.getProperty(i + "_phon_weight", "0.0"));

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
            e.printStackTrace();
        }
        return configModel;
    }
}
