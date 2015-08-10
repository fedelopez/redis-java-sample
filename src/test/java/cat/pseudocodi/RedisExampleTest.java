package cat.pseudocodi;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author fede
 */
public class RedisExampleTest {

    @Test
    public void helloWorldTest() {
        Jedis jedis = new Jedis("localhost");
        jedis.set("foo", "bar");
        String value = jedis.get("foo");
        System.out.println("value = " + value);
    }

    @Test
    public void multiThreadTest() {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
        try (Jedis jedis = pool.getResource()) {
            jedis.set("foo", "bar");
            String foobar = jedis.get("foo");
            System.out.println("foobar = " + foobar);
            jedis.zadd("sose", 0, "car");
            jedis.zadd("sose", 0, "bike");
            Set<String> sose = jedis.zrange("sose", 0, -1);
            System.out.println("value = " + sose);
        }
        /// ... when closing your application:
        pool.destroy();
    }

    @Test
    public void executorServiceTest() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
        executorService.execute(new RedisProducer(4, jedisPool));

        executorService.awaitTermination(5, TimeUnit.SECONDS);
        executorService.shutdown();

        try (Jedis jedis = jedisPool.getResource()) {
            for (int i = 0; i < 1_000; i++) {
                String foo11 = jedis.get("foo-a" + i);
                String foo12 = jedis.get("foo-b" + i);
                String foo13 = jedis.get("foo-c" + i);
                System.out.println(foo11 + ", " + foo12 + ", " + foo13);
            }
        }

        jedisPool.destroy();
    }

    class RedisProducer implements Runnable {
        private final ExecutorService pool;
        private final JedisPool jedisPool;

        public RedisProducer(int poolSize, JedisPool jedisPool) throws IOException {
            this.pool = Executors.newFixedThreadPool(poolSize);
            this.jedisPool = jedisPool;
        }

        public void run() {
            for (int i = 0; i < 1_000; i++) {
                pool.execute(new PersistKeyHandler(jedisPool, "a" + i));
                pool.execute(new PersistKeyHandler(jedisPool, "b" + i));
                pool.execute(new PersistKeyHandler(jedisPool, "c" + i));
            }
        }
    }

    class PersistKeyHandler implements Runnable {
        private final JedisPool jedisPool;
        private final String id;

        PersistKeyHandler(JedisPool jedisPool, String id) {
            this.jedisPool = jedisPool;
            this.id = id;
        }

        @Override
        public void run() {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.set("foo-" + id, "bar-" + id);
            }
        }
    }


}
