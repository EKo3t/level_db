package shadow.leveldb;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import shadow.leveldb.mapping.LevelDbMapper;

import java.io.File;
import java.io.IOException;

import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class LevelDb {

    private final DB instance;
    private final LevelDbMapper mapper;

    public LevelDb(LevelDbMapper mapper) throws IOException {
        Options options = new Options();
        instance = factory.open(new File("levelDBStore"), options);
        this.mapper = mapper;
    }

    public LevelDb(LevelDbMapper mapper, Options options, String absolutePath) throws IOException {
        instance = factory.open(new File(absolutePath), options);
        this.mapper = mapper;
    }

    public <TKey, TValue> void save(TKey key, TValue value) {
        instance.put(mapper.toByteArray(key), mapper.toByteArray(value));
    }

    public <T> T get(String key, Class<T> tClass) {
        return mapper.toTarget(instance.get(bytes(key)), tClass);
    }

    public <TKey> void delete(TKey key) {
        instance.delete(mapper.toByteArray(key));
    }

    public void applyBatch(LevelDbTransaction transaction) throws IOException {
        WriteBatch batch = instance.createWriteBatch();
        for (LevelDbTransaction.LevelDbOperation operation : transaction.getOperations()) {
            byte[] keyAsBytes = mapper.toByteArray(operation.getKey());
            if (operation.hasValue()) {
                byte[] valueAsBytes = mapper.toByteArray(operation.getValue());
                batch.put(keyAsBytes, valueAsBytes);
            } else {
                batch.delete(keyAsBytes);
            }
        }
        instance.write(batch);
        batch.close();
    }
}
