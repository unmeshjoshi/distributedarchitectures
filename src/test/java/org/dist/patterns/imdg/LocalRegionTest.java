package org.dist.patterns.imdg;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class LocalRegionTest {

    static class TransactionCoordinator {
        List<LocalRegion> regions = new ArrayList<>();

        public TransactionCoordinator(List<LocalRegion> regions) {
            this.regions = regions;
        }

        TransactionId id;
        List<LocalRegion> regionsInTxn = new ArrayList<>();

        public TransactionId begin() {
            id = new TransactionId(UUID.randomUUID());
            return id;
        }

        public void put(String key, String value) {
            LocalRegion<String, String> region = getRegionFor(key);
            regionsInTxn.add(region);
            region.put(id, key, value);
        }

        public void commit() {
            for (LocalRegion localRegion : regionsInTxn) {
                localRegion.commit(id);
            }
        }

        private LocalRegion<String, String> getRegionFor(String key) {
            return regions.get(0);
        }


    }

    @Test
    public void transactionalPut() {
        LocalRegion<String, String> region = new LocalRegion("R1");
        TransactionCoordinator coordinator = new TransactionCoordinator(Arrays.asList(region));
        coordinator.begin();

        coordinator.put("key1", "value1");

        assertNull(region.get("key1")); //should not be able to read uncommitted values

        coordinator.commit();
        String value = region.get("key1");
        assertEquals("value1", value);
    }
}