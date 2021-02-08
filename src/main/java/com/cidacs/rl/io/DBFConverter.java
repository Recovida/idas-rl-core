package com.cidacs.rl.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFUtils;

public class DBFConverter {

    protected static String enquote(String s) {
        return '"' + s.replaceAll("\"", "\"\"") + '"';
    }


    public static String toCSV(String dbfFileName) {
        DBFReader dbf = null;
        File tempFile;
        try {
            tempFile = File.createTempFile(dbfFileName + "_", ".csv");
        } catch (IOException e1) {
            e1.printStackTrace();
            return null;
        }
        tempFile.deleteOnExit();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile))) {
            dbf = new DBFReader(new FileInputStream(dbfFileName));
            int n = dbf.getFieldCount();
            StringBuilder row = new StringBuilder();
            for (int i = 0; i < n; i++)
                row.append(i > 0 ? "," : "").append(enquote(dbf.getField(i).getName()));
            bw.write(row.append('\n').toString());
            Object[] rowObjects;
            while ((rowObjects = dbf.nextRecord()) != null) {
                row = new StringBuilder();
                for (int i = 0; i < rowObjects.length; i++)
                    row.append(i > 0 ? "," : "").append(enquote(rowObjects[i] != null ? rowObjects[i].toString() : ""));
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
