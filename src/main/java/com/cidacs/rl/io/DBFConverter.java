package com.cidacs.rl.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFUtils;

public class DBFConverter {

    protected static String quote(String s) {
        return '"' + s.replaceAll("\"", "\"\"") + '"';
    }

    public static Iterable<DatasetRecord> getDatasetRecordIterable(String dbfFileName, String encoding) {
        try {
            return new Iterable<DatasetRecord>() {

                DBFReader dbf = new DBFReader(new FileInputStream(dbfFileName), Charset.forName(encoding));
                Map<String, Integer> keyToIndex = IntStream.range(0, dbf.getFieldCount()).boxed().collect(Collectors.toMap(i -> dbf.getField(i).getName(), Function.identity()));

                @Override
                public Iterator<DatasetRecord> iterator() {

                    return new Iterator<DatasetRecord>() {

                        Object[] nextItem = dbf.nextRecord();

                        @Override
                        public boolean hasNext() {
                            return nextItem != null;
                        }

                        @Override
                        public DatasetRecord next() {
                            Object[] r = nextItem;
                            nextItem = dbf.nextRecord();
                            return DatasetRecord.fromKeyToIndexAndArray(keyToIndex, nextItem);
                        }
                    };

                }
            };
        } catch (IOException | DBFException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static String toCSV(String dbfFileName, String encoding) {
        DBFReader dbf = null;
        File tempFile;
        try {
            tempFile = File.createTempFile(dbfFileName + "_", ".csv");
        } catch (IOException e1) {
            e1.printStackTrace();
            return null;
        }
        tempFile.deleteOnExit();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile, Charset.forName(encoding)))) {
            dbf = new DBFReader(new FileInputStream(dbfFileName), Charset.forName(encoding));
            int n = dbf.getFieldCount();
            StringBuilder row = new StringBuilder();
            for (int i = 0; i < n; i++)
                row.append(i > 0 ? "," : "").append(quote(dbf.getField(i).getName()));
            bw.write(row.append('\n').toString());
            Object[] rowObjects;
            while ((rowObjects = dbf.nextRecord()) != null) {
                row = new StringBuilder();
                for (int i = 0; i < rowObjects.length; i++)
                    row.append(i > 0 ? "," : "").append(quote(rowObjects[i] != null ? rowObjects[i].toString() : ""));
                bw.write(row.append('\n').toString());
            }
        } catch (DBFException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            DBFUtils.close(dbf);
        }
        return tempFile.getAbsolutePath();
    }

}
