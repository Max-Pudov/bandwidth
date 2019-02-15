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
    public void init(ServiceContext ctx) throws Exception {
    }

    @Override
    public void cancel(ServiceContext ctx) {
    }

    @Override
    public void execute(ServiceContext ctx) {
        while (true) {
            try {
                IgniteCache<String, PhoneCall> phoneCalls = ignite.cache("PhoneCalls");
                IgniteCache<String, PhoneCall> phoneCallsBackup = ignite.cache("PhoneCallsBackup");

                long now = System.currentTimeMillis();

                phoneCalls.localEntries(CachePeekMode.PRIMARY).forEach(e -> {
                    String key = e.getKey();
                    PhoneCall call = e.getValue();

                    if (call.getEndTime() == -1 &&
                        now - call.getStartTime() >= expirationTime &&
                        !phoneCallsBackup.containsKey(key)) {

                        phoneCallsBackup.put(key, call);
                    }
                });
            }
            catch (Exception e) {
                log.error(e.getMessage());
            }

            try {
                Thread.sleep(updateInterval);
            }
            catch (InterruptedException e) {
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
