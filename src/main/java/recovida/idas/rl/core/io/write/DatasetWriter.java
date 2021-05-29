package recovida.idas.rl.core.io.write;

public interface DatasetWriter extends AutoCloseable {

    boolean writeRow(String row);

    @Override
    void close();

}
