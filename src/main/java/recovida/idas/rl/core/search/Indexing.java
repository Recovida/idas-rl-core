package recovida.idas.rl.core.search;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import recovida.idas.rl.core.config.ColumnConfigModel;
import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.io.DatasetRecord;
import recovida.idas.rl.core.record.ColumnRecordModel;
import recovida.idas.rl.core.record.RecordModel;
import recovida.idas.rl.core.util.Cleaner;
import recovida.idas.rl.core.util.Phonetic;

public class Indexing {

    public enum IndexingStatus {
        NONE, CORRUPT, INCOMPLETE, COMPLETE, DIFFERENT_CLEANING_PATTERN
    }

    ConfigModel config;
    
    IndexWriter inWriter;
    
    IndexingStatus status = IndexingStatus.NONE;
    
    Collection<String> missingColumnsInExistingIndex = Collections.emptyList();
    
    Collection<String> missingColumnsInDataset = Collections.emptyList();
    
    Collection<String> columnsInDataset = Collections.emptyList();

    public Collection<String> getColumnsInDataset() {
        return Collections.unmodifiableCollection(columnsInDataset);
    }

    public Collection<String> getMissingColumnsInDataset() {
        return Collections.unmodifiableCollection(missingColumnsInDataset);
    }

    protected long indexedEntries = 0;

    public Indexing(ConfigModel config) {
        this.config = config;
        status = checkIndexingStatus();
    }

    public IndexingStatus getIndexingStatus() {
        return status;
    }

    public IndexingStatus checkIndexingStatus() {
        indexedEntries = 0;
        Path dbIndexPath = Paths.get(config.getDbIndex());
        if (!Files.isDirectory(dbIndexPath))
            return IndexingStatus.NONE;
        Path successPath = getSuccessFilePath();
        if (!Files.isRegularFile(successPath))
            return IndexingStatus.CORRUPT;
        Map<String, String> alreadyIndexedColumns = getIndexedColumns(
                successPath);
        Map<String, String> columnsToIndex = getColumnsToIndex();
        missingColumnsInExistingIndex = columnsToIndex.keySet().stream()
                .filter(c -> !Objects.equals(alreadyIndexedColumns.get(c),
                        columnsToIndex.get(c)))
                .collect(Collectors.toList());
        if (!missingColumnsInExistingIndex.isEmpty())
            return IndexingStatus.INCOMPLETE;
        if (!Objects.equals(config.getCleaningRegex(),
                getCleaningRegexOnIndex(getCleaningPatternFilePath())))
            return IndexingStatus.DIFFERENT_CLEANING_PATTERN;
        FSDirectory index = null;
        DirectoryReader reader = null;
        try {
            index = FSDirectory.open(Paths.get(config.getDbIndex()));
            if (index == null)
                return IndexingStatus.CORRUPT;
            reader = DirectoryReader.open(index);
            indexedEntries = reader.numDocs();
        } catch (IOException e) {
            return IndexingStatus.CORRUPT;
        } finally {
            try {
                if (index != null)
                    index.close();
                if (reader != null)
                    reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return IndexingStatus.COMPLETE;
    }

    protected static Map<String, String> getIndexedColumns(Path successPath) {
        try {
            return Arrays
                    .stream(new String(Files.readAllBytes(successPath),
                            Charset.forName("UTF-8")).split("\\n"))
                    .filter(l -> l.indexOf(',') >= 0).map(l -> l.split(",", 2))
                    .collect(Collectors.toMap(s -> s[0], s -> s[1]));
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    protected static String getCleaningRegexOnIndex(Path cleaningRegexPath) {
        try {
            return new String(Files.readAllBytes(cleaningRegexPath),
                    Charset.forName("UTF-8"));
        } catch (IOException e) {
            return "";
        }
    }

    protected Map<String, String> getColumnsToIndex() {
        return config.getColumns().stream().filter(
                c -> !("copy".equals(c.getType()) && "".equals(c.getIndexB())))
                .collect(Collectors.toMap(c -> c.getId(), c -> c.getIndexB()));
    }

    protected Path getSuccessFilePath() {
        return Paths.get(config.getDbIndex()).resolve("_COMPLETE");
    }

    protected Path getCleaningPatternFilePath() {
        return Paths.get(config.getDbIndex()).resolve("_CLEANING");
    }

    public synchronized boolean index(Iterable<DatasetRecord> records,
            Cleaner cleaner) {
        indexedEntries = 0;
        missingColumnsInExistingIndex = Collections.emptyList();
        missingColumnsInDataset = Collections.emptyList();
        columnsInDataset = Collections.emptyList();
        Path dbIndexPath = Paths.get(config.getDbIndex());
        Path successPath = getSuccessFilePath();

        Map<String, String> columnsToIndex = getColumnsToIndex();

        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig idxConfig = new IndexWriterConfig(analyzer);

        Directory index = null;
        try {
            index = FSDirectory.open(dbIndexPath);
            inWriter = new IndexWriter(index, idxConfig);

            for (DatasetRecord record : records) {
                if (Thread.currentThread().isInterrupted())
                    return false;
                if (columnsInDataset.isEmpty()) // first time - save column list
                    columnsInDataset = record.getKeySet();
                RecordModel tmpRecordModel = fromDatasetRecordToRecordModel(
                        ++indexedEntries, record, cleaner);
                if (tmpRecordModel == null) {
                    missingColumnsInDataset = columnsToIndex.values().stream()
                            .filter(c -> !c.isEmpty()
                                    && !c.equals(config.getRowNumColNameB())
                                    && !columnsInDataset.contains(c))
                            .collect(Collectors.toList());
                    return false;
                }
                if (!addRecordToIndex(tmpRecordModel))
                    return false;
            }

            inWriter.close();

            try (BufferedWriter bw = Files.newBufferedWriter(successPath)) { // uses
                                                                             // UTF-8
                for (Entry<String, String> idAndIndexB : columnsToIndex
                        .entrySet())
                    bw.write(idAndIndexB.getKey() + ',' + idAndIndexB.getValue()
                            + '\n');
            }

            try (BufferedWriter bw = Files
                    .newBufferedWriter(getCleaningPatternFilePath())) { // uses
                                                                        // UTF-8
                bw.write(cleaner.getNameCleaningPattern().pattern());
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (inWriter != null)
                    inWriter.close();
            } catch (IOException e) {
            }
            try {
                if (index != null)
                    index.close();
            } catch (IOException e) {
            }
        }
        return true;
    }

    private boolean addRecordToIndex(RecordModel record) {
        Document doc = new Document();
        for (ColumnRecordModel column : record.getColumnRecordModels()) {
            doc.add(new TextField(column.getId(), column.getValue(),
                    Field.Store.YES));
        }
        try {
            inWriter.addDocument(doc);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private RecordModel fromDatasetRecordToRecordModel(long num,
            DatasetRecord datasetRecord, Cleaner cleaner) {
        ColumnRecordModel tmpRecordColumnRecord;
        String tmpIndex;
        String cleanedValue = null;
        String tmpValue = null;
        String tmpId;
        String tmpType;
        ArrayList<ColumnRecordModel> tmpRecordColumns;

        tmpRecordColumns = new ArrayList<>();
        for (ColumnConfigModel column : config.getColumns()) {
            if (column.isGenerated())
                continue;
            tmpIndex = column.getIndexB();
            String originalValue;
            try {
                if (column.getType().equals("copy")) {
                    originalValue = tmpIndex.equals("") ? ""
                            : datasetRecord.get(tmpIndex);
                    cleanedValue = originalValue;
                    tmpValue = originalValue;
                } else {
                    originalValue = tmpIndex.equals(config.getRowNumColNameB())
                            ? String.valueOf(num)
                            : datasetRecord.get(tmpIndex);
                    cleanedValue = cleaner.clean(column, originalValue);
                    tmpValue = cleanedValue.replaceAll("[^A-Z0-9 /]", "")
                            .replaceAll("\\s+", " ").trim();
                    if (!"date".equals(column.getType()))
                        tmpValue = tmpValue.replace('/', ' ');
                }
            } catch (IllegalArgumentException e) {
                return null;
            }
            tmpId = column.getId();
            tmpType = column.getType();

            tmpRecordColumnRecord = new ColumnRecordModel(tmpId, tmpType,
                    tmpValue);
            if (column.getType().equals("copy") && tmpIndex.equals(""))
                tmpRecordColumnRecord.setGenerated(true);
            tmpRecordColumns.add(tmpRecordColumnRecord);

            double phonWeight = column.getPhonWeight();
            if (tmpType.equals("name") && phonWeight > 0) {
                ColumnRecordModel c = new ColumnRecordModel(tmpId + "__PHON__",
                        "string", Phonetic.convert(cleanedValue));
                c.setGenerated(true);
                tmpRecordColumns.add(c);
            }
        }
        RecordModel recordModel = new RecordModel(tmpRecordColumns);
        return recordModel;
    }

    public boolean deleteOldIndex() {
        File f = new File(config.getDbIndex());
        try {
            FileUtils.deleteDirectory(f);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public long numIndexedEntries() {
        return indexedEntries;
    }
}
