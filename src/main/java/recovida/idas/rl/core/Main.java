package recovida.idas.rl.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;

import recovida.idas.rl.core.config.ColumnConfigModel;
import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.config.ConfigReader;
import recovida.idas.rl.core.io.DatasetRecord;
import recovida.idas.rl.core.io.read.CSVDatasetReader;
import recovida.idas.rl.core.io.read.DBFDatasetReader;
import recovida.idas.rl.core.io.read.DatasetReader;
import recovida.idas.rl.core.io.write.CSVDatasetWriter;
import recovida.idas.rl.core.io.write.DatasetWriter;
import recovida.idas.rl.core.linkage.Linkage;
import recovida.idas.rl.core.linkage.LinkageUtils;
import recovida.idas.rl.core.record.ColumnRecordModel;
import recovida.idas.rl.core.record.RecordModel;
import recovida.idas.rl.core.search.Indexing;
import recovida.idas.rl.core.search.Indexing.IndexingStatus;
import recovida.idas.rl.core.util.Cleaning;
import recovida.idas.rl.core.util.Phonetic;
import recovida.idas.rl.core.util.StatusReporter;

public class Main {

    public static boolean execute(String configFileName, int progressReportIntervals) {

        // read configuration file
        ConfigReader confReader = new ConfigReader();

        if (!new File(configFileName).isFile()) {
            StatusReporter.get().errorConfigFileDoesNotExist(configFileName);
            return false;
        }
        StatusReporter.get().infoUsingConfigFile(configFileName);
        ConfigModel config = confReader.readConfig(configFileName);

        if (config == null)
            return false;

        String fileName_a = config.getDbA();
        String fileName_b = config.getDbB();

        if (!new File(fileName_a).isFile()) {
            StatusReporter.get().errorDatasetFileDoesNotExist(fileName_a);
            return false;
        }

        if (!new File(fileName_b).isFile()) {
            StatusReporter.get().errorDatasetFileDoesNotExist(fileName_b);
            return false;
        }

        // read dataset A (just to check for errors and get the number of rows)
        StatusReporter.get().infoReadingA(fileName_a);
        DatasetReader readerA = null;
        if (fileName_a.toLowerCase().endsWith(".csv")) {
            char delimiter_a = CSVDatasetReader.guessCsvDelimiter(fileName_a,
                    config.getEncodingA());
            if (delimiter_a == '\0') {
                StatusReporter.get().errorDatasetFileCannotBeRead(fileName_a,
                        config.getEncodingA());
                return false;
            }
            readerA = new CSVDatasetReader(fileName_a, delimiter_a,
                    config.getEncodingA());
        } else if (fileName_a.toLowerCase().endsWith(".dbf")) {
            readerA = new DBFDatasetReader(fileName_a, config.getEncodingA());
        } else {
            StatusReporter.get()
            .errorDatasetFileFormatIsUnsupported(fileName_a);
            return false;
        }
        Iterable<DatasetRecord> dbARecords = readerA.getDatasetRecordIterable();
        if (dbARecords == null) {
            StatusReporter.get().errorDatasetFileCannotBeRead(fileName_a,
                    config.getEncodingA());
            return false;
        }
        long n = 0;
        Iterator<DatasetRecord> it = dbARecords.iterator();
        if (it.hasNext()) {
            n++;
            // make sure all columns are present
            Collection<String> keySet = it.next().getKeySet();
            Collection<String> missing = config.getColumns().stream()
                    .filter(m -> !m.isGenerated() && !m.getIndexA().isEmpty()
                            && !m.getIndexA()
                            .equals(config.getRowNumColNameA()))
                    .map(m -> m.getIndexA()).filter(c -> !keySet.contains(c))
                    .collect(Collectors.toSet());
            if (!missing.isEmpty()) {
                StatusReporter.get().infoAvailableColumnsInDatasetA(
                        '"' + String.join("\", \"", keySet) + '"');
                missing.stream().forEach(col -> StatusReporter.get()
                        .errorMissingColumnInDatasetA(col));
                return false;
            }
        }
        while (it.hasNext()) {
            it.next();
            n++;
        }
        StatusReporter.get().infoFinishedReadingA(n);

        // read dataset B
        StatusReporter.get().infoReadingAndIndexingB(config.getDbB());

        Iterable<DatasetRecord> dbBRecords = null;
        DatasetReader readerB = null;
        if (fileName_b.toLowerCase().endsWith(".csv")) {
            char delimiter_b = CSVDatasetReader.guessCsvDelimiter(fileName_b,
                    config.getEncodingB());
            if (delimiter_b == '\0') {
                StatusReporter.get().errorDatasetFileCannotBeRead(fileName_b,
                        config.getEncodingB());
                return false;
            }
            readerB = new CSVDatasetReader(fileName_b, delimiter_b,
                    config.getEncodingB());
        } else if (fileName_b.toLowerCase().endsWith(".dbf")) {
            readerB = new DBFDatasetReader(fileName_b, config.getEncodingB());
        } else {
            StatusReporter.get()
            .errorDatasetFileFormatIsUnsupported(fileName_b);
            return false;
        }
        dbBRecords = readerB.getDatasetRecordIterable();
        if (dbBRecords == null) {
            StatusReporter.get().errorDatasetFileCannotBeRead(fileName_b,
                    config.getEncodingB());
            return false;
        }

        // prepare indexing
        config.setDbIndex(
                config.getDbIndex() + File.separator + getHash(fileName_b));
        Indexing indexing = new Indexing(config);
        IndexingStatus indexingStatus = indexing.getIndexingStatus();
        switch (indexingStatus) {
        case COMPLETE:
            StatusReporter.get().infoReusingIndex(indexing.numIndexedEntries());
            break;
        case CORRUPT:
            StatusReporter.get().infoOldIndexIsCorrupt();
            break;
        case INCOMPLETE:
            StatusReporter.get().infoOldIndexLacksColumns();
            break;
        case NONE:
            break;
        }
        if (indexingStatus == IndexingStatus.CORRUPT
                || indexingStatus == IndexingStatus.INCOMPLETE) {
            if (!indexing.deleteOldIndex()) {
                StatusReporter.get().errorOldIndexCannotBeDeleted(
                        config.getDbIndex().toString());
                return false;
            }
        }
        if (indexingStatus != IndexingStatus.COMPLETE) {
            if (!indexing.index(dbBRecords)) {
                Collection<String> missing = indexing
                        .getMissingColumnsInDataset();
                if (!missing.isEmpty()) {
                    StatusReporter.get()
                    .infoAvailableColumnsInDatasetB('"'
                            + String.join("\", \"",
                                    indexing.getColumnsInDataset())
                            + '"');
                    missing.stream().forEach(col -> StatusReporter.get()
                            .errorMissingColumnInDatasetB(col));
                } else // generic error
                    StatusReporter.get().errorCannotIndex(Paths
                            .get(config.getDbIndex()).getParent().toString());
                return false;
            }
            StatusReporter.get()
            .infoFinishedIndexingB(indexing.numIndexedEntries());
        }

        // prepare to read dataset A again
        Iterable<DatasetRecord> records = readerA.getDatasetRecordIterable();
        if (records == null) {
            StatusReporter.get().errorDatasetFileCannotBeRead(fileName_a,
                    config.getEncodingA());
            return false;
        }

        String resultPath = new File(config.getLinkageDir() + File.separator
                + new java.text.SimpleDateFormat("yyyyMMdd-HHmmss")
                .format(java.util.Calendar.getInstance().getTime()))
                .getPath();

        if (config.getMaxRows() < Long.MAX_VALUE) {
            StatusReporter.get().infoMaxRowsA(config.getMaxRows());
            if (n > config.getMaxRows())
                n = config.getMaxRows();
        }

        final int maxThreads = config.getThreadCount();
        StatusReporter.get().infoPerformingLinkage(maxThreads);

        Linkage linkage = new Linkage(config);

        final int BUFFER_SIZE = 1000;

        ExecutorService pool = Executors.newFixedThreadPool(maxThreads);
        BlockingQueue<Future<String>> q = new ArrayBlockingQueue<>(BUFFER_SIZE);

        new Thread(() -> {
            long readRows = 0;
            for (DatasetRecord row : records) {
                Callable<String> fn = () -> {
                    if (row.getNumber() > config.getMaxRows())
                        return "...";
                    // place holder variables to instantiate an record object
                    RecordModel tmpRecord = new RecordModel();
                    ArrayList<ColumnRecordModel> tmpRecordColumns = new ArrayList<>();

                    // convert row to RecordModel
                    for (ColumnConfigModel column : config.getColumns()) {
                        if (column.isGenerated()
                                || (column.getType().equals("copy")
                                        && column.getIndexA().equals("")))
                            continue;
                        String tmpType = column.getType();
                        String originalValue, cleanedValue, tmpValue;
                        if (tmpType.equals("copy")) {
                            originalValue = column.getIndexA().equals("") ? ""
                                    : row.get(column.getIndexA());
                            cleanedValue = originalValue;
                            tmpValue = originalValue;
                        } else {
                            originalValue = column.getIndexA()
                                    .equals(config.getRowNumColNameA())
                                    ? String.valueOf(row.getNumber())
                                            : row.get(column.getIndexA());
                            cleanedValue = Cleaning.clean(column,
                                    originalValue);
                            // Remove anything that is not an upper-case letter,
                            // slash, space or digit
                            tmpValue = cleanedValue
                                    .replaceAll("[^A-Z0-9 /]", "")
                                    .replaceAll("\\s+", " ").trim();
                        }
                        String tmpId = column.getId();

                        // add new column
                        tmpRecordColumns.add(new ColumnRecordModel(tmpId,
                                tmpType, tmpValue));
                        double phonWeight = column.getPhonWeight();
                        if (tmpType.equals("name") && phonWeight > 0) {
                            ColumnRecordModel c = new ColumnRecordModel(
                                    tmpId + "__PHON__", "string",
                                    Phonetic.convert(cleanedValue));
                            c.setGenerated(true);
                            tmpRecordColumns.add(c);
                        }
                    }
                    // set the column to record
                    tmpRecord.setColumnRecordModels(tmpRecordColumns);
                    return linkage.linkSpark(tmpRecord);
                };
                try {
                    q.put(pool.submit(fn));
                } catch (InterruptedException e) {
                }
                if (++readRows == config.getMaxRows())
                    break;
            }
        }).start();

        DatasetWriter writer = new CSVDatasetWriter(
                resultPath + File.separator + "result.csv", ';');
        String header = LinkageUtils.getCsvHeaderFromConfig(config);
        if (!writer.writeRow(header)) {
            StatusReporter.get().errorCannotSaveResult();
            return false;
        }
        long reportEvery = Math.max(n / progressReportIntervals, 1);
        for (int i = 1; i <= n; i++) {
            try {
                Future<String> f = q.take();
                if (!writer.writeRow(f.get())) {
                    StatusReporter.get().errorCannotSaveResult();
                    return false;
                }
                if (i == 1 || i == n || i % reportEvery == 0)
                    StatusReporter.get().infoLinkageProgress((float) i / n);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
                writer.close();
                pool.shutdown();
                return false;
            }
        }
        writer.close();
        pool.shutdown();
        StatusReporter.get().infoCompleted(resultPath);
        return true;
    }

    public static void main(String[] args) {
        String configFileName = new File(
                args.length < 1 ? "assets/config.properties" : args[0])
                .getPath();
        if (!execute(configFileName, 100))
            System.exit(1);
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
