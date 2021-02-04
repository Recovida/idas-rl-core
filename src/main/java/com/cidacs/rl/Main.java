package com.cidacs.rl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.cidacs.rl.config.ColumnConfigModel;
import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.config.ConfigReader;
import com.cidacs.rl.io.CsvHandler;
import com.cidacs.rl.linkage.Linkage;
import com.cidacs.rl.record.ColumnRecordModel;
import com.cidacs.rl.record.RecordModel;
import com.cidacs.rl.search.Indexing;

import scala.Tuple2;




public class Main {

    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.WARN);


        ConfigReader confReader = new ConfigReader();
        ConfigModel config = confReader.readConfig();

        String fileName_a = config.getDbA();
        String fileName_b = config.getDbB();

        String firstLine_a = Files.lines(Path.of(fileName_a)).findFirst().get();
        String firstLine_b = Files.lines(Path.of(fileName_b)).findFirst().get();

        char delimiter_a = guessCsvDelimiter(firstLine_a);
        char delimiter_b = guessCsvDelimiter(firstLine_b);

        CsvHandler csvHandler = new CsvHandler();
        Indexing indexing = new Indexing(config);

        // read database B
        System.out.println("Reading and indexing dataset B...");
        Iterable<CSVRecord> dbBCsvRecords;
        dbBCsvRecords = csvHandler.getCsvIterable(config.getDbB(), delimiter_b);
        indexing.index(dbBCsvRecords);
        System.out.println("Finished indexing.");


        // Start Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Cidacs-RL")
                .config("spark.master", "local[*]")
                .getOrCreate();

        // read dataset A
        Dataset<Row> dsa = spark.read().format("csv")
                .option("sep", "" + delimiter_a)
                .option("inferSchema", "false")
                .option("header", "true")
                .load(config.getDbA());

        String resultPath = "assets/linkage-" + new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(java.util.Calendar.getInstance().getTime());

        System.out.println("Performing integration...");

        dsa.javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, String>() {

            @Override
            public String call(Tuple2<Row, Long> r){
                Row row = r._1;
                // Reading config again
                // Using the config from outer scope throws java not serializable error
                ConfigReader confReader = new ConfigReader();
                ConfigModel config = confReader.readConfig();
                // same with linkage
                Linkage linkage = new Linkage(config);
                if (r._2 >= config.getMaxRows())
                    return "...";

                // place holder variables to instantiate an record object
                RecordModel tmpRecord = new RecordModel();
                ArrayList<ColumnRecordModel> tmpRecordColumns = new ArrayList<>();

                // convert row to RecordModel
                for(ColumnConfigModel column : config.getColumns()){
                    if (column.isGenerated())
                        continue;
                    try {
                        String originalValue = row.getAs(column.getIndexA());
                        String tmpValue = Cleaning.clean(column, originalValue);
                        // Remove anything that is not a uppercase letter and a digit
                        tmpValue = tmpValue.replaceAll("[^A-Z0-9 ]", "").replaceAll("\\s+", " ").trim();
                        String tmpId = column.getId();
                        String tmpType = column.getType();
                        // add new column
                        tmpRecordColumns.add(new ColumnRecordModel(tmpId, tmpType, tmpValue, originalValue));
                        double phonWeight = column.getPhonWeight();
                        if (tmpType.equals("name") && phonWeight > 0) {
                            ColumnRecordModel c = new ColumnRecordModel(tmpId + "__PHON__", "string", Phonetic.convert(originalValue), "");
                            c.setGenerated(true);
                            tmpRecordColumns.add(c);
                        }
                    } catch (ArrayIndexOutOfBoundsException e){
                        e.printStackTrace();
                    }
                }
                // set the column to record
                tmpRecord.setColumnRecordModels(tmpRecordColumns);
                return linkage.linkSpark(tmpRecord);
            }
            private static final long serialVersionUID = 1L;
        }).saveAsTextFile(resultPath);
        // Write header to file
        csvHandler.writeHeaderFromConfig(resultPath + "/header.csv", config);
        System.out.println("Completed.");
        spark.stop();
    }

    private static char guessCsvDelimiter(String firstLine) throws IOException {
        char[] delimiters = {',', ';', '|', '\t'};
        char delimiter = '\0';
        long occurrences = -1;
        for (char sep : delimiters) {
            long n = firstLine.chars().filter(ch -> ch == sep).count();
            if (n > occurrences) {
                delimiter = sep;
                occurrences = n;
            }
        }
        return delimiter;
    }

}
