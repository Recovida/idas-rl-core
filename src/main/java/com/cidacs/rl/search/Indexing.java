package com.cidacs.rl.search;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
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

        if (Files.exists(dbIndexPath)){
            Logger.getLogger(getClass()).info("There is a database already indexed with the name provided. No indexing is necessary.");
            return 0;
        } else {
            StandardAnalyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig config = new IndexWriterConfig(analyzer);

            Directory index = null;
            long n = 0;
            try {
                index = FSDirectory.open(dbIndexPath);
                this.inWriter = new IndexWriter(index, config);

                for (CSVRecord csvRecord : csvRecords) {
                    tmpRecordModel = this.fromCSVRecordToRecord(csvRecord);
                    this.addRecordToIndex(tmpRecordModel);
                    n++;
                }

                this.inWriter.close();
                return n;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return -1;
        }
    }

    private void addRecordToIndex(RecordModel record){
        Document doc = new Document();
        for(ColumnRecordModel column: record.getColumnRecordModels()){
            doc.add(new TextField(column.getId(), column.getValue(),  Field.Store.YES));
            doc.add(new StoredField(column.getId() + "___ORIG___", column.getOriginalValue()));
        }
        try {
            this.inWriter.addDocument(doc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private RecordModel fromCSVRecordToRecord(CSVRecord csvRecord){
        ColumnRecordModel tmpRecordColumnRecord;
        String tmpIndex;
        String tmpValue;
        String tmpId;
        String tmpType;
        ArrayList<ColumnRecordModel> tmpRecordColumns;

        tmpRecordColumns = new ArrayList<>();
        for(ColumnConfigModel column : config.getColumns()) {
            if (column.isGenerated())
                continue;
            tmpIndex = column.getIndexB();
            String originalValue = csvRecord.get(tmpIndex);
            tmpValue = Cleaning.clean(column, originalValue);
            tmpValue = tmpValue.replaceAll("[^A-Z0-9 ]", "").replaceAll("\\s+", " ").trim();
            tmpId = column.getId();
            tmpType = column.getType();

            tmpRecordColumnRecord = new ColumnRecordModel(tmpId, tmpType, tmpValue, originalValue);
            tmpRecordColumns.add(tmpRecordColumnRecord);

            double phonWeight = column.getPhonWeight();
            if (tmpType.equals("name") && phonWeight > 0) {
                ColumnRecordModel c = new ColumnRecordModel(tmpId + "__PHON__", "string", Phonetic.convert(originalValue), "");
                c.setGenerated(true);
                tmpRecordColumns.add(c);
            }
        }
        RecordModel recordModel = new RecordModel(tmpRecordColumns);
        return recordModel;
    }
}
