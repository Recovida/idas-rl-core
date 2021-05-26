package recovida.idas.rl.search;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
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

import recovida.idas.rl.config.ColumnConfigModel;
import recovida.idas.rl.config.ConfigModel;
import recovida.idas.rl.io.DatasetRecord;
import recovida.idas.rl.record.ColumnRecordModel;
import recovida.idas.rl.record.RecordModel;
import recovida.idas.rl.util.Cleaning;
import recovida.idas.rl.util.Phonetic;

public class Indexing {

    public enum IndexingStatus {
        NONE, CORRUPT, INCOMPLETE, COMPLETE
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
        this.status = checkIndexingStatus();
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
        Collection<String> alreadyIndexedColumns = getIndexedColumns(
                successPath);
        missingColumnsInExistingIndex = getColumnsToIndex().stream()
                .filter(c -> !alreadyIndexedColumns.contains(c))
                .collect(Collectors.toList());
        if (!missingColumnsInExistingIndex.isEmpty())
            return IndexingStatus.INCOMPLETE;
        try {
            FSDirectory index = FSDirectory
                    .open(Paths.get(config.getDbIndex()));
            if (index == null)
                return IndexingStatus.CORRUPT;
            DirectoryReader reader = DirectoryReader.open(index);
            indexedEntries = reader.numDocs();
        } catch (IOException e) {
            return IndexingStatus.CORRUPT;
        }
        return IndexingStatus.COMPLETE;
    }

    protected static Collection<String> getIndexedColumns(Path successPath) {
        try {
            return new LinkedHashSet<>(Arrays.asList(
                    new String(Files.readAllBytes(successPath)).split("\\n")));
        } catch (IOException e) {
            return Collections.emptySet();
        }
    }

    protected Collection<String> getColumnsToIndex() {
        return config.getColumns().stream().filter(c -> !c.isGenerated()
                && !(c.getType().equals("copy") && c.getIndexB().equals("")))
                .map(c -> c.getIndexB()).collect(Collectors.toList());
    }

    protected Path getSuccessFilePath() {
        return Paths.get(config.getDbIndex()).resolve("_COMPLETE");
    }

    public synchronized boolean index(Iterable<DatasetRecord> records) {
        indexedEntries = 0;
        missingColumnsInExistingIndex = Collections.emptyList();
        missingColumnsInDataset = Collections.emptyList();
        columnsInDataset = Collections.emptyList();
        Path dbIndexPath = Paths.get(config.getDbIndex());
        Path successPath = getSuccessFilePath();

        Collection<String> columnsToIndex = getColumnsToIndex();

        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig idxConfig = new IndexWriterConfig(analyzer);

        Directory index = null;
        try {
            index = FSDirectory.open(dbIndexPath);
            this.inWriter = new IndexWriter(index, idxConfig);

            for (DatasetRecord record : records) {
                if (columnsInDataset.isEmpty()) // first time - save column list
                    columnsInDataset = record.getKeySet();
                RecordModel tmpRecordModel = fromDatasetRecordToRecordModel(
                        ++indexedEntries, record);
                if (tmpRecordModel == null) {
                    missingColumnsInDataset = columnsToIndex.stream()
                            .filter(c -> !c.equals(config.getRowNumColNameB())
                                    && !columnsInDataset.contains(c))
                            .collect(Collectors.toList());
                    return false;
                }
                if (!addRecordToIndex(tmpRecordModel))
                    return false;
            }

            this.inWriter.close();

            try (BufferedWriter bw = new BufferedWriter(
                    new FileWriter(successPath.toFile()))) {
                for (String col : columnsToIndex)
                    bw.write(col + '\n');
            }
        } catch (IOException e) {
            return false;
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
            this.inWriter.addDocument(doc);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    private RecordModel fromDatasetRecordToRecordModel(long num,
            DatasetRecord datasetRecord) {
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
                    cleanedValue = Cleaning.clean(column, originalValue);
                    tmpValue = cleanedValue.replaceAll("[^A-Z0-9 /]", "")
                            .replaceAll("\\s+", " ").trim();
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
            return false;
        }
    }

    public long numIndexedEntries() {
        return indexedEntries;
    }
}
