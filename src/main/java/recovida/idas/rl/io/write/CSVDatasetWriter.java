package recovida.idas.rl.io.write;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import recovida.idas.rl.util.StatusReporter;

public class CSVDatasetWriter implements DatasetWriter {

    protected String fileName;
    protected char delimiter;
    protected BufferedWriter bw;

    public CSVDatasetWriter(String fileName, char delimiter) {
        this.fileName = fileName;
        this.delimiter = delimiter;
        new File(fileName).getParentFile().mkdirs();
        try {
            bw = new BufferedWriter(new FileWriter(fileName));
        } catch (IOException e) {
            StatusReporter.get().errorCannotSaveResult();
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void writeRow(String row) {
        try {
            bw.write(row + "\n");
        } catch (IOException e) {
            StatusReporter.get().errorCannotSaveResult();
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void close() {
        try {
            bw.close();
        } catch (IOException e) {
        }
    }


}
