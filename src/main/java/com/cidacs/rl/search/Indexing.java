package com.cidacs.rl.search;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

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

        if (Files.exists(dbIndexPath)) {
            if (Files.exists(successPath)) {
                Logger.getLogger(getClass()).info("Database B has already been indexing. Reusing index.");
                return 0;
            } else {
                Logger.getLogger(getClass()).info("Indexing of database B has probably been interrupted in a previous execution. Indexing it again.");
                try {
                    FileUtils.deleteDirectory(dbIndexPath.toFile());
                } catch (IOException e) {
                    Logger.getLogger(getClass()).error(
                            String.format("Could not delete old index. Please delete the directory “%s” and try again.", dbIndexPath.toString()));
                    e.printStackTrace();
                    System.exit(1);
                }
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

            successPath.toFile().createNewFile();
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
            originalValue = tmpIndex.equals(config.getRowNumColNameB()) ? String.valueOf(num) : csvRecord.get(tmpIndex);
            cleanedValue = Cleaning.clean(column, originalValue);
            tmpValue = cleanedValue.replaceAll("[^A-Z0-9 /]", "").replaceAll("\\s+", " ").trim();
            tmpId = column.getId();
            tmpType = column.getType();

            tmpRecordColumnRecord = new ColumnRecordModel(tmpId, tmpType, tmpValue);
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
}
