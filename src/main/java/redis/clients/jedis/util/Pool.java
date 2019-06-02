package redis.clients.jedis.util;

import java.io.Closeable;
import java.util.NoSuchElementException;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisExhaustedPoolException;

public abstract class Pool<T> implements Closeable {

  protected GenericObjectPool<T> internalPool;

  protected Pool() {
  }

  /**
   * 通过此方法来构建连接池
   * @param poolConfig 连接池的一些公共配置属性
   * @param factory 池化对象工厂，用来创建连接池中池化对象
   */
  public Pool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {
    initPool(poolConfig, factory);
  }

  @Override
  public void close() {
    destroy();
  }

  /**
   * 连接池是否已关闭
   */
  public boolean isClosed() {
    return this.internalPool.isClosed();
  }

  /**
   * 创建连接池对象，注意：如果之前已经初始化过则会先关闭再重新创建
    * @param poolConfig
   * @param factory
   */
  public void initPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {

    if (this.internalPool != null) {
      try {
        closeInternalPool();
      } catch (Exception e) {
      }
    }

    this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
  }


  /**
   * 获取连接池中的资源
   */
  public T getResource() {
    try {
      return internalPool.borrowObject();
    } catch (NoSuchElementException nse) {
      if (null == nse.getCause()) { // 连接池耗尽时会为NPE，获取不到元素
        throw new JedisExhaustedPoolException(
            "Could not get a resource since the pool is exhausted", nse);
      }
      // Otherwise, the exception was caused by the implemented activateObject() or ValidateObject()
      throw new JedisException("Could not get a resource from the pool", nse);
    } catch (Exception e) {
      throw new JedisConnectionException("Could not get a resource from the pool", e);
    }
  }

  /**
   * 归还资源到连接池中
   */
  protected void returnResourceObject(final T resource) {
    if (resource == null) {
      return;
    }
    try {
      internalPool.returnObject(resource);
    } catch (Exception e) {
      throw new JedisException("Could not return the resource to the pool", e);
    }
  }

  protected void returnBrokenResource(final T resource) {
    if (resource != null) {
      returnBrokenResourceObject(resource);
    }
  }

  protected void returnResource(final T resource) {
    if (resource != null) {
      returnResourceObject(resource);
    }
  }


  /**
   * 关闭连接池
   */
  public void destroy() {
    closeInternalPool();
  }

  protected void returnBrokenResourceObject(final T resource) {
    try {
      internalPool.invalidateObject(resource);
    } catch (Exception e) {
      throw new JedisException("Could not return the broken resource to the pool", e);
    }
  }

  protected void closeInternalPool() {
    try {
      internalPool.close();
    } catch (Exception e) {
      throw new JedisException("Could not destroy the pool", e);
    }
  }
  
  /**
   * 返回连接池中可以借用的实例数，-1 代表连接池不是激活状态
   */
  public int getNumActive() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getNumActive();
  }
  
  /**
   * 返回当前连接池中闲置的资源数量，-1代表当前连接池不是激活状态
   */
  public int getNumIdle() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getNumIdle();
  }
  
  /**
   * Returns an estimate of the number of threads currently blocked waiting for
   * a resource from this pool.
   *
   * @return The number of threads waiting, -1 if the pool is inactive.
   */
  public int getNumWaiters() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getNumWaiters();
  }
  
  /**
   * Returns the mean waiting time spent by threads to obtain a resource from
   * this pool.
   *
   * @return The mean waiting time, in milliseconds, -1 if the pool is
   * inactive.
   */
  public long getMeanBorrowWaitTimeMillis() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getMeanBorrowWaitTimeMillis();
  }
  
  /**
   * Returns the maximum waiting time spent by threads to obtain a resource
   * from this pool.
   *
   * @return The maximum waiting time, in milliseconds, -1 if the pool is
   * inactive.
   */
  public long getMaxBorrowWaitTimeMillis() {
    if (poolInactive()) {
      return -1;
    }

    return this.internalPool.getMaxBorrowWaitTimeMillis();
  }

  private boolean poolInactive() {
    return this.internalPool == null || this.internalPool.isClosed();
  }

  public void addObjects(int count) {
    try {
      for (int i = 0; i < count; i++) {
        this.internalPool.addObject();
      }
    } catch (Exception e) {
      throw new JedisException("Error trying to add idle objects", e);
    }
  }
}
