package au.csiro.pathling.terminology;

import au.csiro.pathling.utilities.Preconditions;
import org.apache.curator.utils.CloseableUtils;
import java.io.Closeable;
import java.io.Serializable;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ObjectHolder<K extends Serializable, V> {


  V getOrCreate(@Nonnull final K config, @Nonnull final Function<K, V> create);

  void invalidate();

  class SingletonHolder<K extends Serializable, V> implements ObjectHolder<K, V> {

    @Nullable
    private K configuration;
    @Nullable
    private V instance;

    @Override
    @Nonnull
    public synchronized V getOrCreate(@Nonnull final K config,
        @Nonnull final Function<K, V> create) {
      if (configuration == null) {
        instance = create.apply(config);
        configuration = config;
      } else {
        Preconditions.check(configuration.equals(config),
            "Attempt to create SingletonHolder with different configuration");
      }
      return Preconditions.checkNotNull(instance);
    }

    @Override
    public synchronized void invalidate() {
      if (instance instanceof Closeable) {
        CloseableUtils.closeQuietly((Closeable) instance);
        instance = null;
        configuration = null;
      }
    }

    static <K extends Serializable, V> ObjectHolder<K, V> singleton() {
      return new SingletonHolder<>();
    }
  }


}
