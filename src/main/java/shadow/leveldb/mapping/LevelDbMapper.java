package shadow.leveldb.mapping;

public interface LevelDbMapper {

    <TSource> byte[] toByteArray(TSource source);

    <TTarget> TTarget toTarget(byte[] bytes, Class<TTarget> targetClass);
}
