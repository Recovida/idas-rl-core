package recovida.idas.rl.search;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import recovida.idas.rl.Cleaning;
import recovida.idas.rl.Phonetic;
import recovida.idas.rl.config.ColumnConfigModel;
import recovida.idas.rl.config.ConfigModel;
import recovida.idas.rl.io.DatasetRecord;
import recovida.idas.rl.record.ColumnRecordModel;
import recovida.idas.rl.record.RecordModel;
import recovida.idas.rl.util.StatusReporter;

public class Indexing {
    ConfigModel config;
    IndexWriter inWriter;

    public Indexing(ConfigModel config) {
        this.config = config;
    }

    public long index(Iterable<DatasetRecord> csvRecords){
        RecordModel tmpRecordModel;

        Path dbIndexPath = Paths.get(config.getDbIndex());
        Path successPath = Paths.get(dbIndexPath + File.separator + "_COMPLETE");

        Set<String> columnsToIndex = new HashSet<>();
        for (ColumnConfigModel column : this.config.getColumns())
            if (!column.isGenerated() && !(column.getType().equals("copy") && column.getIndexB().equals("")))
                columnsToIndex.add(column.getIndexB());

        if (Files.exists(dbIndexPath)) {
            if (Files.exists(successPath)) {
                Set<String> indexedColumns = new HashSet<>();
                try {
                    for (String c : new String(Files.readAllBytes(successPath)).split("\\n"))
                        indexedColumns.add(c);
                } catch (IOException e) {
                }
                boolean allIndexed = false;
                if (indexedColumns.size() >= columnsToIndex.size()) {
                    allIndexed = true;
                    for (String c : columnsToIndex)
                        if (!indexedColumns.contains(c)) {
                            allIndexed = false;
                            break;
                        }
                }
                if (allIndexed) {
                    StatusReporter.get().infoReusingIndex();
                    return 0;
                } else {
                    StatusReporter.get().infoOldIndexLacksColumns();
                    deleteOldIndex(dbIndexPath.toFile());
                }
            } else {
                StatusReporter.get().infoOldIndexIsCorrupt();
                deleteOldIndex(dbIndexPath.toFile());
            }
        }
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        Directory index = null;
        long n = 0;
        try {
            index = FSDirectory.open(dbIndexPath);
            this.inWriter = new IndexWriter(index, config);

            for (DatasetRecord csvRecord : csvRecords) {
                tmpRecordModel = this.fromDatasetRecordToRecordModel(++n, csvRecord);
                this.addRecordToIndex(tmpRecordModel);
            }

            this.inWriter.close();

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(successPath.toFile()))) {
                for (String col : columnsToIndex)
                    bw.write(col + '\n');
            }
            return n;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private void addRecordToIndex(RecordModel record){
        Document doc = new Document();
        for(ColumnRecordModel column: record.getColumnRecordModels()){
            doc.add(new TextField(column.getId(), column.getValue(),  Field.Store.YES));
        }
        try {
            this.inWriter.addDocument(doc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private RecordModel fromDatasetRecordToRecordModel(long num, DatasetRecord datasetRecord){
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
                    originalValue = tmpIndex.equals("") ? "" : datasetRecord.get(tmpIndex);
                    cleanedValue = originalValue;
                    tmpValue = originalValue;
                } else {
                    originalValue = tmpIndex.equals(config.getRowNumColNameB()) ? String.valueOf(num) : datasetRecord.get(tmpIndex);
                    cleanedValue = Cleaning.clean(column, originalValue);
                    tmpValue = cleanedValue.replaceAll("[^A-Z0-9 /]", "").replaceAll("\\s+", " ").trim();
                }
            } catch (IllegalArgumentException e) {
                StatusReporter.get()
                .errorMissingColumnInDatasetB(tmpIndex);
                StatusReporter.get().infoAvailableColumnsInDatasetB(
                        '"' + String.join("\", \"", datasetRecord.getKeySet()) + '"');
                System.exit(1);
            }
            tmpId = column.getId();
            tmpType = column.getType();

            tmpRecordColumnRecord = new ColumnRecordModel(tmpId, tmpType, tmpValue);
            if (column.getType().equals("copy") && tmpIndex.equals(""))
                tmpRecordColumnRecord.setGenerated(true);
            tmpRecordColumns.add(tmpRecordColumnRecord);

            double phonWeight = column.getPhonWeight();
            if (tmpType.equals("name") && phonWeight > 0) {
                ColumnRecordModel c = new ColumnRecordModel(tmpId + "__PHON__", "string", Phonetic.convert(cleanedValue));
                c.setGenerated(true);
                tmpRecordColumns.add(c);
            }
        }
        RecordModel recordModel = new RecordModel(tmpRecordColumns);
        return recordModel;
    }

    private void deleteOldIndex(File f) {
        try {
            FileUtils.deleteDirectory(f);
        } catch (IOException e) {
            StatusReporter.get().errorOldIndexCannotBeDeleted(f.toString());
            System.exit(1);
        }
    }
}
