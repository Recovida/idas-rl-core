package com.cidacs.rl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import org.apache.commons.csv.CSVRecord;
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




public class Main {

    public static void main(String[] args) throws IOException {
        ConfigReader confReader = new ConfigReader();
        ConfigModel config = confReader.readConfig();

        String fileName_a = args[0];
        String fileName_b = args[1];

        String firstLine_a = Files.lines(Path.of(fileName_a)).findFirst().get();
        String firstLine_b = Files.lines(Path.of(fileName_b)).findFirst().get();

        char delimiter_a = guessCsvDelimiter(firstLine_a);
        char delimiter_b = guessCsvDelimiter(firstLine_b);

        CsvHandler csvHandler = new CsvHandler();
        Indexing indexing = new Indexing(config);

        // read database B
        Iterable<CSVRecord> dbBCsvRecords;
        dbBCsvRecords = csvHandler.getCsvIterable(config.getDbB(), delimiter_b);

        indexing.index(dbBCsvRecords);


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

        dsa.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row){
                // Reading config again
                // Using the config from outer scope throws java not serializable error
                ConfigReader confReader = new ConfigReader();
                ConfigModel config = confReader.readConfig();
                // same with linkage
                Linkage linkage = new Linkage(config);

                // place holder variables to instantiate an record object
                RecordModel tmpRecord = new RecordModel();
                ArrayList<ColumnRecordModel> tmpRecordColumns = new ArrayList<>();

                // convert row to RecordModel
                for(ColumnConfigModel column : config.getColumns()){
                    try {
                        String originalValue = row.getAs(column.getIndexA());
                        String tmpValue = Cleaning.clean(column, originalValue);
                        // Remove anything that is not a uppercase letter and a digit
                        tmpValue = tmpValue.replaceAll("[^A-Z0-9 ]", "").replaceAll("\\s+", " ").trim();
                        //
                        String tmpId = column.getId();
                        String tmpType = column.getType();
                        // add new column
                        tmpRecordColumns.add(new ColumnRecordModel(tmpId, tmpType, tmpValue, originalValue));
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

        csvHandler.writeHeaderFromConfig(resultPath + "/header.csv", config);
        //
        // Write header to file
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
