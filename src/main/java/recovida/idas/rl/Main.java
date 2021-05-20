package recovida.idas.rl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.iterators.IteratorChain;
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

import recovida.idas.rl.config.ColumnConfigModel;
import recovida.idas.rl.config.ConfigModel;
import recovida.idas.rl.config.ConfigReader;
import recovida.idas.rl.io.CsvHandler;
import recovida.idas.rl.io.DBFConverter;
import recovida.idas.rl.io.DatasetRecord;
import recovida.idas.rl.linkage.Linkage;
import recovida.idas.rl.linkage.LinkageUtils;
import recovida.idas.rl.record.ColumnRecordModel;
import recovida.idas.rl.record.RecordModel;
import recovida.idas.rl.search.Indexing;
import recovida.idas.rl.util.StatusReporter;
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
            StatusReporter.get().errorConfigFileDoesNotExist(configFileName);
            System.exit(1);
        }
        StatusReporter.get().infoUsingConfigFile(configFileName);
        ConfigModel config = confReader.readConfig(configFileName);


        // convert file if not CSV
        String fileName_a = config.getDbA();
        String fileName_b = config.getDbB();
        fileName_a = convertFileIfNeeded(fileName_a, config.getEncodingA());
        config.setDbA(fileName_a);

        // read first line and guess delimiter
        char delimiter_a = guessCsvDelimiter(fileName_a, config.getEncodingA());




        Iterable<DatasetRecord> dbBCsvRecords = null;
        if (fileName_b.toLowerCase().endsWith(".csv")) {
            char delimiter_b = guessCsvDelimiter(fileName_b, config.getEncodingB());
            dbBCsvRecords = CsvHandler.getDatasetRecordIterable(config.getDbB(), delimiter_b, config.getEncodingB());
        } else if (fileName_b.toLowerCase().endsWith(".dbf")) {
            dbBCsvRecords = DBFConverter.getDatasetRecordIterable(fileName_b, config.getEncodingB());
        } else {
            StatusReporter.get().errorDatasetFileFormatIsUnsupported(fileName_b);
            System.exit(1);
        }

        // prepare indexing
        config.setDbIndex(config.getDbIndex() + File.separator + getHash(fileName_b));

        Indexing indexing = new Indexing(config);

        // read database B
        StatusReporter.get().infoReadingAndIndexingB(config.getDbB());


        long count_b = indexing.index(dbBCsvRecords);
        if (count_b > 0)
            StatusReporter.get().infoFinishedIndexingB(count_b);

        disableIllegalAccessWarnings();

        // Start Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Cidacs-RL")
                .config("spark.master", "local[*]").config("spark.testing.memory", "2000000000")
                .getOrCreate();

        // read dataset A
        StatusReporter.get().infoReadingA(config.getDbA());
        Dataset<Row> dsa = spark.read().format("csv")
                .option("sep", "" + delimiter_a)
                .option("encoding", config.getEncodingA())
                .option("inferSchema", "false")
                .option("header", "true")
                .load(config.getDbA());
        StatusReporter.get().infoFinishedReadingA(dsa.count());

        String resultPath = new File(config.getLinkageDir() + File.separator + new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(java.util.Calendar.getInstance().getTime())).getPath();

        if (config.getMaxRows() < Long.MAX_VALUE)
            StatusReporter.get().infoMaxRowsA(config.getMaxRows());

        StatusReporter.get().infoPerformingLinkage();

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
                    if (column.isGenerated() || (column.getType().equals("copy") && column.getIndexA().equals("")))
                        continue;
                    try {
                        String tmpType = column.getType();
                        String originalValue, cleanedValue, tmpValue;
                        if (tmpType.equals("copy")) {
                            originalValue = column.getIndexA().equals("") ? "" : row.getAs(column.getIndexA());
                            cleanedValue = originalValue;
                            tmpValue = originalValue;
                        } else {
                            originalValue = column.getIndexA().equals(config.getRowNumColNameA()) ? String.valueOf(r._2 + 1) : row.getAs(column.getIndexA());
                            cleanedValue = Cleaning.clean(column, originalValue);
                            // Remove anything that is not an uppercase letter, slash, space or digit
                            tmpValue = cleanedValue.replaceAll("[^A-Z0-9 /]", "").replaceAll("\\s+", " ").trim();
                        }
                        String tmpId = column.getId();

                        // add new column
                        tmpRecordColumns.add(new ColumnRecordModel(tmpId, tmpType, tmpValue));
                        double phonWeight = column.getPhonWeight();
                        if (tmpType.equals("name") && phonWeight > 0) {
                            ColumnRecordModel c = new ColumnRecordModel(tmpId + "__PHON__", "string", Phonetic.convert(cleanedValue));
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
        StatusReporter.get().infoCompleted(resultPath);
        spark.stop();
    }


    private static String convertFileIfNeeded(String fileName, String encoding) {
        if (!new File(fileName).isFile()) {
            StatusReporter.get().errorDatasetFileDoesNotExist(fileName);
            System.exit(1);
        }
        if (fileName.toLowerCase().endsWith(".csv")) {
            // do nothing
        } else if (fileName.toLowerCase().endsWith(".dbf")) {
            Logger.getLogger(Main.class).info(String.format("Converting file \"%s\" to CSV...", fileName));
            String newFileName = DBFConverter.toCSV(fileName, encoding);
            if (newFileName == null) {
                StatusReporter.get().errorDatasetFileCannotBeRead(fileName);
                System.exit(1);
            }
            return newFileName;
        } else {
            StatusReporter.get().errorDatasetFileFormatIsUnsupported(fileName);
            System.exit(1);
        }
        return fileName;
    }

    private static char guessCsvDelimiter(String fileName, String encoding) throws IOException {
        String firstLine = null;
        try {
            firstLine = Files.lines(Paths.get(fileName), Charset.forName(encoding)).findFirst().get();
        } catch (UncheckedIOException e) {
            StatusReporter.get().errorDatasetFileCannotBeRead(fileName, encoding);
            System.exit(1);
        }
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
