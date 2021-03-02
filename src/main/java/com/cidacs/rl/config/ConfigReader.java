package com.cidacs.rl.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;


public class ConfigReader {


    public ConfigModel readConfig(String propFileName){
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

            // read dba, dbb and dbIndex
            configModel.setDbA(config.getProperty("db_a"));
            configModel.setDbB(config.getProperty("db_b"));
            configModel.setSuffixA(config.getProperty("suffix_a"));
            configModel.setSuffixB(config.getProperty("suffix_b"));
            configModel.setDbIndex(config.getProperty("db_index"));
            configModel.setLinkageDir(config.getProperty("linkage_folder"));
            if (config.containsKey("max_rows"))
                configModel.setMaxRows(Long.valueOf(config.getProperty("max_rows")));
            if (config.containsKey("min_score"))
                configModel.setMinimumScore(Float.valueOf(config.getProperty("min_score")) / 100);

            // read all columns
            for(int i=0; i<=20; i++){
                String id = config.getProperty(i+"_id");
                String type = config.getProperty(i+"_type");
                String indexA = config.getProperty(i+"_index_a");
                String indexB = config.getProperty(i+"_index_b");

                // if any of the columns is missing the columns is thrown away
                if(id==null || type==null || indexA==null || indexB==null || config.getProperty(i+"_weight")==null){
                    if (id != null || type != null || indexA != null || indexB != null)
                        Logger.getLogger(getClass()).warn(String.format("Ignoring column %d_ because some items are missing.\n", i));
                    continue;
                }

                double weight = Double.valueOf(config.getProperty(i+"_weight"));
                double phonWeight = Double.valueOf(config.getProperty(i + "_phon_weight", "0.0"));

                configModel.addColumn(new ColumnConfigModel(id, type, indexA, indexB, weight, phonWeight));

                if (type.equals("name") && phonWeight > 0) {
                    ColumnConfigModel c = new ColumnConfigModel(id + "__PHON__", "string", "", "", phonWeight, 0.0);
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
