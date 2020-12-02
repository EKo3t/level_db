package shadow.leveldb;

import java.util.Collections;
import java.util.List;

public class LevelDbTransaction {

    private List<LevelDbOperation> operations;

    private LevelDbTransaction() {
    }

    public static LevelDbTransaction create() {
        return new LevelDbTransaction();
    }

    public <T1, T2> LevelDbTransaction addWrite(T1 key, T2 value) {
        operations.add(LevelDbOperation.put(key, value));
        return this;
    }

    public <T> LevelDbTransaction addDelete(T key) {
        operations.remove(LevelDbOperation.delete(key));
        return this;
    }

    public List<LevelDbOperation> getOperations() {
        return Collections.unmodifiableList(operations);
    }

    public static class LevelDbOperation {

        private final Object key;
        // can be null
        private final Object value;

        public LevelDbOperation(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

        public static LevelDbOperation delete(Object key) {
            return new LevelDbOperation(key, null);
        }

        public static LevelDbOperation put(Object key, Object value) {
            return new LevelDbOperation(key, value);
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public boolean hasValue() {
            return value != null;
        }
    }
}
