package bandwidth;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

public class BackupService implements Service {

    private volatile int expirationTime;
    private volatile int updateInterval;

    @IgniteInstanceResource
    private Ignite ignite;

    @LoggerResource
    private IgniteLogger log;

    @Override
    public void cancel(ServiceContext ctx) {
        //no-op
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        IgniteCache<String, Long> phoneCalls = ignite.cache("PhoneCalls");
        IgniteCache<String, Long> phoneCallsBackup = ignite.cache("PhoneCallsBackup");
        phoneCallsBackup.localEntries(CachePeekMode.PRIMARY).forEach(e -> phoneCalls.put(e.getKey(), e.getValue()));
    }

    @Override
    public void execute(ServiceContext ctx) {
        while (true) {
            try {
                IgniteCache<String, Long> phoneCalls = ignite.cache("PhoneCalls");
                IgniteCache<String, Long> phoneCallsBackup = ignite.cache("PhoneCallsBackup");
                phoneCalls.localEntries(CachePeekMode.PRIMARY).forEach(e -> {
                    long expirationTimestamp = System.currentTimeMillis() - expirationTime + updateInterval;
                    if (e.getValue() < expirationTimestamp && !phoneCallsBackup.containsKey(e.getKey())) {
                        phoneCallsBackup.put(e.getKey(), e.getValue());
                        if (!phoneCalls.containsKey(e.getKey())) {
                            phoneCallsBackup.remove(e.getKey());
                        }
                    }
                });
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            try {
                Thread.currentThread().sleep(updateInterval);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    public void setExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
    }

    public void setUpdateInterval(int updateInterval) {
        this.updateInterval = updateInterval;
    }
}
