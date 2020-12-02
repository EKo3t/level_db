package shadow.leveldb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import java.io.File;
import java.io.IOException;

import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class LevelDb {

    private final DB instance;
    private final ObjectMapper mapper;

    public LevelDb(ObjectMapper mapper) throws IOException {
        Options options = new Options();
        instance = factory.open(new File("levelDBStore"), options);
        this.mapper = mapper;
    }

    public LevelDb(ObjectMapper mapper, Options options, String absolutePath) throws IOException {
        instance = factory.open(new File(absolutePath), options);
        this.mapper = mapper;
    }

    public <TKey, TValue> void save(TKey key, TValue value) throws JsonProcessingException {
        instance.put(mapper.writeValueAsBytes(key), mapper.writeValueAsBytes(value));
    }

    public <T> T get(String key, Class<T> tClass) throws IOException {
        return mapper.readValue(instance.get(bytes(key)), tClass);
    }

    public <TKey> void delete(TKey key) throws JsonProcessingException {
        instance.delete(mapper.writeValueAsBytes(key));
    }

    public void applyBatch(LevelDbTransaction transaction) throws IOException {
        WriteBatch batch = instance.createWriteBatch();
        for (LevelDbTransaction.LevelDbOperation operation : transaction.getOperations()) {
            byte[] keyAsBytes = mapper.writeValueAsBytes(operation.getKey());
            if (operation.hasValue()) {
                byte[] valueAsBytes = mapper.writeValueAsBytes(operation.getValue());
                batch.put(keyAsBytes, valueAsBytes);
            } else {
                batch.delete(keyAsBytes);
            }
        }
        instance.write(batch);
        batch.close();
    }
}
