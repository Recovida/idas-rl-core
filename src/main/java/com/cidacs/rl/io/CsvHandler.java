package com.cidacs.rl.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.spark.api.java.JavaRDD;

import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.linkage.LinkageUtils;
import com.cidacs.rl.util.StatusReporter;


public class CsvHandler {
    public static Iterable<DatasetRecord> getDatasetRecordIterable(String csvPath, char delimiter,
            String encoding) {
        Reader in = null;
        try {
            FileInputStream fis = new FileInputStream(csvPath);
            InputStream isWithoutBOM = new BOMInputStream(fis,
                    ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                    ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                    ByteOrderMark.UTF_32BE);
            in = new InputStreamReader(isWithoutBOM, Charset.forName(encoding));
            Iterable<CSVRecord> records = CSVFormat.RFC4180
                    .withFirstRecordAsHeader().withDelimiter(delimiter)
                    .parse(in);

            return DatasetRecord.fromCSVRecordIterable(records);
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
            StatusReporter.get().errorCannotSaveResult();
            e.printStackTrace();
        }
        // close file
    }

    public static void writeRDDasCSV(JavaRDD<String> data, String fileName) {
        new File(fileName).getParentFile().mkdirs();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName))) {
            List<String> rows = data.collect();
            StatusReporter.get().infoSavingResult();
            for (String row : rows)
                bw.write(row + "\n");
        } catch (IOException e) {
            StatusReporter.get().errorCannotSaveResult();
            e.printStackTrace();
            System.exit(1);
        }
    }
}
