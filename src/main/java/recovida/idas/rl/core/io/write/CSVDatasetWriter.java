package recovida.idas.rl.core.io.write;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CSVDatasetWriter implements DatasetWriter {

    protected String fileName;
    protected char delimiter;
    protected BufferedWriter bw;

    public CSVDatasetWriter(String fileName, char delimiter) {
        this.fileName = fileName;
        this.delimiter = delimiter;
        new File(fileName).getParentFile().mkdirs();
    }

    @Override
    public boolean writeRow(String row) {
        try {
            if (bw == null) {
                bw = new BufferedWriter(new FileWriter(fileName));
            }
            bw.write(row + "\n");
            return true;
        } catch (IOException e) {
            return false;
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
