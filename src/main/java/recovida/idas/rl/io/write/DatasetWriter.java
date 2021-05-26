package recovida.idas.rl.io.write;

public interface DatasetWriter {

    boolean writeRow(String row);

    void close();

}
