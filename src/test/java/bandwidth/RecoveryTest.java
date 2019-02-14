package bandwidth;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

public class RecoveryTest {

    @Test
    public void checkSurvivalAfterRestart() throws Exception {
        //start a cluster with one single node
        Ignition.start("test-config.xml").active(true);

        //start a client
        Ignite client = Ignition.start("client-config.xml");
        IgniteCache<String, Long> phoneCalls = client.cache("PhoneCalls");
        IgniteCache<Object, Object> phoneCallsBackup = client.cache("PhoneCallsBackup");

        phoneCalls.removeAll();
        phoneCallsBackup.removeAll();

        //put 2 keys in in-memory cache
        String key1 = UUID.randomUUID().toString();
        phoneCalls.put(key1, System.currentTimeMillis());

        String key2 = UUID.randomUUID().toString();
        phoneCalls.put(key2, System.currentTimeMillis());

        //wait for keys being copied to persistent cache
        Thread.currentThread().sleep(15_000);

        assertEquals(phoneCallsBackup.size(), 2);

        //remove second key, note that it should be removed from both caches
        phoneCalls.remove(key2);
        phoneCallsBackup.remove(key2);

        //put third key in a cache
        String key3 = UUID.randomUUID().toString();
        phoneCalls.put(key3, System.currentTimeMillis());

        //wait less than expiration time
        Thread.currentThread().sleep(5_000);

        assertEquals(1, phoneCallsBackup.size());

        //stop all nodes
        Ignition.stopAll(true);
        Ignite server = Ignition.start("test-config.xml");;

        assertEquals(server.cache("PhoneCallsBackup").size(), 1);
        phoneCalls = server.cache("PhoneCalls");

        //ensure that key1 was backed up and other 2 keys did not survive
        assertTrue(phoneCalls.containsKey(key1));
        assertFalse(phoneCalls.containsKey(key2));
        assertFalse(phoneCalls.containsKey(key3));

        server.cache("PhoneCallsBackup").removeAll();
    }
}
