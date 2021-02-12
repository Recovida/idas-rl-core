package com.cidacs.rl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.cidacs.rl.config.ColumnConfigModel;
import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.config.ConfigReader;
import com.cidacs.rl.io.CsvHandler;
import com.cidacs.rl.io.DBFConverter;
import com.cidacs.rl.linkage.Linkage;
import com.cidacs.rl.linkage.LinkageUtils;
import com.cidacs.rl.record.ColumnRecordModel;
import com.cidacs.rl.record.RecordModel;
import com.cidacs.rl.search.Indexing;

import scala.Tuple2;
import sun.misc.Unsafe;


public class Main {

    final static Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR);
        Logger.getLogger("org.sparkproject").setLevel(Level.ERROR);

        // On Windows, save some required files
        if (SystemUtils.IS_OS_WINDOWS) {
            Path hadoopPath = Files.createTempDirectory("hadoop");
            hadoopPath.toFile().deleteOnExit();
            Path hadoopBinPath = hadoopPath.resolve("bin");
            hadoopBinPath.toFile().mkdirs();
            String[] hadoopFiles = {"hadoop.dll", "winutils.exe"};
            for (String f : hadoopFiles)
                FileUtils.copyInputStreamToFile(Main.class.getClassLoader().getResourceAsStream(f), hadoopBinPath.resolve(f).toFile());
            System.setProperty("hadoop.home.dir", hadoopPath.toAbsolutePath().toString());
        }


        // read configuration file
        ConfigReader confReader = new ConfigReader();
        String configFileName = new File(args.length < 1 ? "assets/config.properties" : args[0]).getPath();
        if (!new File(configFileName).isFile()) {
            logger.error(String.format("Configuration file \"%s\" does not exist.", configFileName));
            System.exit(1);
        }
        logger.info(String.format("Using configuration file \"%s\".", configFileName));
        ConfigModel config = confReader.readConfig(configFileName);


        // convert file if not CSV
        String fileName_a = config.getDbA();
        String fileName_b = config.getDbB();
        fileName_a = convertFileIfNeeded(fileName_a);
        fileName_b = convertFileIfNeeded(fileName_b);
        config.setDbA(fileName_a);
        config.setDbB(fileName_b);

        // read first line and guess delimiter
        String firstLine_a = Files.lines(Paths.get(fileName_a)).findFirst().get();
        String firstLine_b = Files.lines(Paths.get(fileName_b)).findFirst().get();
        char delimiter_a = guessCsvDelimiter(firstLine_a);
        char delimiter_b = guessCsvDelimiter(firstLine_b);

        // prepare indexing
        config.setDbIndex(config.getDbIndex() + File.separator + getHash(fileName_b));
        CsvHandler csvHandler = new CsvHandler();
        Indexing indexing = new Indexing(config);

        // read database B
        logger.info("Reading and indexing dataset B...");
        Iterable<CSVRecord> dbBCsvRecords;
        dbBCsvRecords = csvHandler.getCsvIterable(config.getDbB(), delimiter_b);
        long count_b = indexing.index(dbBCsvRecords);
        if (count_b > 0)
            logger.info("Finished reading and indexing dataset B (entries: " + count_b + ").");

        disableIllegalAccessWarnings();

        // Start Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Cidacs-RL")
                .config("spark.master", "local[*]").config("spark.testing.memory", "2000000000")
                .getOrCreate();

        // read dataset A
        logger.info("Reading dataset A...");
        Dataset<Row> dsa = spark.read().format("csv")
                .option("sep", "" + delimiter_a)
                .option("inferSchema", "false")
                .option("header", "true")
                .load(config.getDbA());
        logger.info("Finished reading dataset A (entries: " + dsa.count() + ").");

        String resultPath = new File(config.getLinkageDir() + File.separator + new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(java.util.Calendar.getInstance().getTime())).getPath();

        if (config.getMaxRows() < Long.MAX_VALUE)
            logger.info(String.format("Linking at most %d item(s).", config.getMaxRows()));

        logger.info("Performing linkage...");

        Linkage linkage = new Linkage(config);

        String header = LinkageUtils.getCsvHeaderFromConfig(config);

        JavaRDD<String> rdd = dsa.javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, String>() {

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
        }).repartition(1).mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator)
                    throws Exception {
                // Includes header
                return new IteratorChain(Arrays.asList(header).iterator(), iterator);
            }
        }, false);
        CsvHandler.writeRDDasCSV(rdd, resultPath + File.separator + "result.csv");
        logger.info(String.format("Completed. The result was saved in \"%s\".", resultPath));
        spark.stop();
    }


    private static String convertFileIfNeeded(String fileName) {
        if (!new File(fileName).isFile()) {
            logger.error(String.format("Could not find file: \"%s\".", fileName));
            System.exit(1);
        }
        if (fileName.toLowerCase().endsWith(".csv")) {
            // do nothing
        } else if (fileName.toLowerCase().endsWith(".dbf")) {
            Logger.getLogger(Main.class).info(String.format("Converting file \"%s\" to CSV...", fileName));
            String newFileName = DBFConverter.toCSV(fileName);
            if (newFileName == null) {
                logger.error(String.format("Could not read file: \"%s\".", fileName));
                System.exit(1);
            }
            return newFileName;
        } else {
            logger.error(String.format("Unsupported format: \"%s\".", fileName));
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

    private static String getHash(String fileName) {
        try (InputStream is = Files.newInputStream(Paths.get(fileName))) {
            return DigestUtils.md5Hex(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
