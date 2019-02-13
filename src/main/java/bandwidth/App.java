package bandwidth;

import org.apache.ignite.*;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;

public class App {

    private static int EXPIRATION_TIME = 600_000;
    private static int UPDATE_INTERVAL = 60_000;

    public static void main(String[] args) throws Exception {
        start(UPDATE_INTERVAL, EXPIRATION_TIME);
    }

    public static Ignite start(int updateInterval, int expirationTime) throws Exception {
        Ignite ignite = IgniteSpring.start("server-config.xml", null);
        ignite.active(true);
        IgniteCache<String, Long> phoneCalls = ignite.cache("PhoneCalls");
        IgniteCache<String, Long> phoneCallsBackup = ignite.cache("PhoneCallsBackup");
        phoneCallsBackup.forEach(e -> phoneCalls.put(e.getKey(), e.getValue()));
        new Thread(new Service(updateInterval, expirationTime, ignite.compute())).start();
        return ignite;
    }

    private static class Service implements Runnable {

        private final int updateInterval;
        private final int expirationTime;
        private final IgniteCompute compute;

        private Service(int updateInterval, int expirationTime, IgniteCompute compute) {
            this.updateInterval = updateInterval;
            this.expirationTime = expirationTime;
            this.compute = compute;
        }

        @Override
        public void run() {
            while (true) {
                compute.broadcast(new Task(), expirationTime + updateInterval);
                try {
                    Thread.currentThread().sleep(updateInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class Task implements IgniteClosure<Integer, Object> {

        /** Injected grid. */
        @IgniteInstanceResource
        private transient Ignite ignite;

        @Override
        public Object apply(Integer expirationTime) {
            IgniteCache<String, Long> phoneCalls = ignite.cache("PhoneCalls");
            IgniteCache<String, Long> phoneCallsBackup = ignite.cache("PhoneCallsBackup");
            phoneCalls.localEntries().forEach(e -> {
                long expirationTimestamp = System.currentTimeMillis() - expirationTime;
                if (e.getValue() < expirationTimestamp && !phoneCallsBackup.containsKey(e.getKey())) {
                    phoneCallsBackup.put(e.getKey(), e.getValue());
                    if (!phoneCalls.containsKey(e.getKey())) {
                        phoneCallsBackup.remove(e.getKey());
                    }
                }
            });
            return null;
        }
    }
}
