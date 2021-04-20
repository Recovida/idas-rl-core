package com.cidacs.rl.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.linkage.LinkageUtils;


public class CsvHandler {
    public Iterable<CSVRecord> getCsvIterable(String csvPath, char delimiter, String encoding) {
        Reader in = null;
        try {
            in = new InputStreamReader(new FileInputStream(csvPath), Charset.forName(encoding));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().withDelimiter(delimiter).parse(in);

            return records;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void writeHeaderFromConfig(String pathToHeader, ConfigModel config)  {
        // get string from config
        String header = LinkageUtils.getCsvHeaderFromConfig(config);

        // open file
        Path path = Paths.get(pathToHeader);

        // write header
        try {
            Files.write(path, (header+"\n").getBytes());
        } catch (IOException e) {
            Logger.getLogger(getClass()).error("Error while writing file.");
        }
        // close file
    }

    public static void writeRDDasCSV(JavaRDD<String> data, String fileName) {
        new File(fileName).getParentFile().mkdirs();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName))) {
            List<String> rows = data.collect();
            Logger.getLogger(CsvHandler.class).info("Saving linkage result...");
            for (String row : rows)
                bw.write(row + "\n");
        } catch (IOException e) {
            Logger.getLogger(CsvHandler.class).error("Could not save file.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
