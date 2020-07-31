package org.hbase.reactor;

import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.NonRecoverableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ajith.km
 */
class RxHBaseClientTest {
    //Field client of type HBaseClient - was not mocked since Mockito doesn't mock a Final class when 'mock-maker-inline' option is not set
    RxHBaseClient rxHBaseClient = new RxHBaseClient(new HBaseClient("localhost"));
    String testTable = "fk_sp_cms_listing_data_namespace";
    String testKey = "LSTABBEUPJ7PESH967CCCU3IH";

    String dummyTable = "dummy_table";

    @Test
    void testEnsureTableExistsHappyPath() {

        Mono<Void> result = rxHBaseClient.ensureTableExists(testTable);
        StepVerifier.create(result).expectNext().verifyComplete();
    }

    @Test
    void testEnsureTableExistsSadPath() {

        Mono<Void> result = rxHBaseClient.ensureTableExists(dummyTable);
        StepVerifier.create(result).expectError(NonRecoverableException.class).verify();
    }

    @Test
    void testGet() {
        Mono<ArrayList<KeyValue>> result = rxHBaseClient.get(new GetRequest(testTable, testKey));
        StepVerifier.create(result).assertNext(row -> Assertions.assertEquals(41, row.size(), "Invalid Row")).verifyComplete();
    }

    @Test
    void testScan() {
        Flux<List<KeyValue>> result = rxHBaseClient.newScanner(testTable).scan();
        StepVerifier.create(result).expectNextCount(94).verifyComplete();
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme