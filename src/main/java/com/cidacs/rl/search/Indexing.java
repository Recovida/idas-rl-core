package com.cidacs.rl.search;

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

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.cidacs.rl.Cleaning;
import com.cidacs.rl.Phonetic;
import com.cidacs.rl.config.ColumnConfigModel;
import com.cidacs.rl.config.ConfigModel;
import com.cidacs.rl.record.ColumnRecordModel;
import com.cidacs.rl.record.RecordModel;

public class Indexing {
    ConfigModel config;
    IndexWriter inWriter;

    public Indexing(ConfigModel config) {
        this.config = config;
    }

    public long index(Iterable<CSVRecord> csvRecords){
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
                    Logger.getLogger(getClass()).info("Database B has already been indexed. Reusing index.");
                    return 0;
                } else {
                    Logger.getLogger(getClass()).info("Database B has already been indexed, but the old index does not contain some of the required columns. Indexing it again.");
                    deleteOldIndex(dbIndexPath.toFile());
                }
            } else {
                Logger.getLogger(getClass()).info("Indexing of database B has probably been interrupted in a previous execution. Indexing it again.");
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

            for (CSVRecord csvRecord : csvRecords) {
                tmpRecordModel = this.fromCSVRecordToRecord(++n, csvRecord);
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

    private RecordModel fromCSVRecordToRecord(long num, CSVRecord csvRecord){
        ColumnRecordModel tmpRecordColumnRecord;
        String tmpIndex;
        String cleanedValue;
        String tmpValue;
        String tmpId;
        String tmpType;
        ArrayList<ColumnRecordModel> tmpRecordColumns;

        tmpRecordColumns = new ArrayList<>();
        for (ColumnConfigModel column : config.getColumns()) {
            if (column.isGenerated())
                continue;
            tmpIndex = column.getIndexB();
            String originalValue;
            if (column.getType().equals("copy")) {
                originalValue = tmpIndex.equals("") ? "" : csvRecord.get(tmpIndex);
                cleanedValue = originalValue;
                tmpValue = originalValue;
            } else {
                originalValue = tmpIndex.equals(config.getRowNumColNameB()) ? String.valueOf(num) : csvRecord.get(tmpIndex);
                cleanedValue = Cleaning.clean(column, originalValue);
                tmpValue = cleanedValue.replaceAll("[^A-Z0-9 /]", "").replaceAll("\\s+", " ").trim();
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
            Logger.getLogger(getClass()).error(
                    String.format("Could not delete old index. Please delete the directory “%s” and try again.", f.toString()));
            e.printStackTrace();
            System.exit(1);
        }
    }
}
