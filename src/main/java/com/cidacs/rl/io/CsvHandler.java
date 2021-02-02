package com.cidacs.rl.io;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.linkage.LinkageUtils;


public class CsvHandler {
    public Iterable<CSVRecord> getCsvIterable(String csvPath, char delimiter) {
        Reader in = null;
        try {
            in = new FileReader(csvPath);
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
        LinkageUtils lu = new LinkageUtils();
        String header = lu.getCsvHeaderFromConfig(config);

        // open file
        Path path = Paths.get(pathToHeader);

        // write header
        try {
            Files.write(path, (header+"\n").getBytes());
        } catch (IOException e) {
            System.out.println("Error while writting file.");
        }
        // close file
    }
}
