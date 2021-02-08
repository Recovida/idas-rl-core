package com.cidacs.rl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
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
import com.cidacs.rl.io.DBFConverter;
import com.cidacs.rl.linkage.Linkage;
import com.cidacs.rl.record.ColumnRecordModel;
import com.cidacs.rl.record.RecordModel;
import com.cidacs.rl.search.Indexing;

import scala.Tuple2;
import sun.misc.Unsafe;


public class Main {

    public static void main(String[] args) throws IOException {

        // read configuration file
        ConfigReader confReader = new ConfigReader();
        if (args.length != 1) {
            System.err.println("Please provide the configuration file name as the first argument.");
            System.exit(1);
        }
        ConfigModel config = confReader.readConfig(args[0]);

        // convert file if not CSV
        String fileName_a = config.getDbA();
        String fileName_b = config.getDbB();
        fileName_a = convertFileIfNeeded(fileName_a);
        fileName_b = convertFileIfNeeded(fileName_b);
        config.setDbA(fileName_a);
        config.setDbB(fileName_b);

        // read first line and guess delimiter
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
        long count_b = indexing.index(dbBCsvRecords);
        if (count_b > 0)
            System.out.println("Finished reading and indexing dataset B (entries: " + count_b + ").");

        disableIllegalAccessWarnings();

        // Start Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Cidacs-RL")
                .config("spark.master", "local[*]")
                .getOrCreate();

        // read dataset A
        System.out.println("Reading dataset A...");
        Dataset<Row> dsa = spark.read().format("csv")
                .option("sep", "" + delimiter_a)
                .option("inferSchema", "false")
                .option("header", "true")
                .load(config.getDbA());
        System.out.println("Finished reading dataset A (entries: " + dsa.count() + ").");

        String resultPath = "assets/linkage-" + new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(java.util.Calendar.getInstance().getTime());

        System.out.println("Performing linkage...");

        Linkage linkage = new Linkage(config);

        dsa.javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, String>() {

            @Override
            public String call(Tuple2<Row, Long> r){
                Row row = r._1;
                if (r._2 >= config.getMaxRows())
                    return "...";

                // place holder variables to instantiate an record object
                RecordModel tmpRecord = new RecordModel();
                ArrayList<ColumnRecordModel> tmpRecordColumns = new ArrayList<>();

                // convert row to RecordModel
                for (ColumnConfigModel column : config.getColumns()){
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


    private static String convertFileIfNeeded(String fileName) {
        if (! new File(fileName).isFile()) {
            System.err.format("Could not find file: %s\n", fileName);
            System.exit(1);
        }
        if (fileName.toLowerCase().endsWith(".csv")) {
            // do nothing
        } else if (fileName.toLowerCase().endsWith(".dbf")) {
            System.out.format("Converting file %s to CSV...\n", fileName);
            String newFileName = DBFConverter.toCSV(fileName);
            if (newFileName == null) {
                System.err.format("Could not read file: %s\n", fileName);
                System.exit(1);
            }
            return newFileName;
        } else {
            System.err.format("Unsupported format: %s\n", fileName);
            System.exit(1);
        }
        return fileName;
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

    private static void disableIllegalAccessWarnings() {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);
            Class<?> c = Class.forName("jdk.internal.module.IllegalAccessLogger");
            unsafe.putObjectVolatile(c, unsafe.staticFieldOffset(c.getDeclaredField("logger")), null);
        } catch (Exception e) {
        }
    }

}
