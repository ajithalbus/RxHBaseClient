package org.hbase.reactor;

import lombok.SneakyThrows;
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

/**
 * @author ajith.km
 */
class RxHBaseClientIntegrationTest {

    RxHBaseClient rxHBaseClient = new RxHBaseClient(new HBaseClient("intergration.hbase.host"));
    String testTable = "test";
    String testKey = "50";

    String dummyTable = "dummy_table";

    // TODO Add tests for other operations.

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
        StepVerifier.create(result).assertNext(row -> Assertions.assertEquals(1, row.size(), "Invalid Row")).verifyComplete();
    }

    @Test
    @SneakyThrows
    void testScan() {

        Flux<ArrayList<KeyValue>> result = rxHBaseClient.newScanner(testTable).scan();
        StepVerifier.create(result).expectNextCount(50).verifyComplete();
    }
}