package recovida.idas.rl.io.write;

public interface DatasetWriter {

    void writeRow(String row);

    void close();

}
