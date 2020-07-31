package org.hbase.reactor;

import com.stumbleupon.async.Deferred;
import org.hbase.async.Config;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * @author ajith.km
 */

public class RxHBaseClient {

    private final HBaseClient client;

    RxHBaseClient(final Config config) {
        this.client = new HBaseClient(config);
    }

    RxHBaseClient(final HBaseClient client) {
        this.client = client;
    }

    public Mono<Void> ensureTableExists(final byte[] table) {

        return Mono.create(emitter -> {
            Deferred<Object> caller = client.ensureTableExists(table);
            EventEmitter.bindEmptyMono(emitter, caller);
        });
    }

    public Mono<Void> ensureTableExists(final String table) {
        return ensureTableExists(table.getBytes(StandardCharsets.UTF_8));
    }

    public Mono<ArrayList<KeyValue>> get(final GetRequest get) {

        return Mono.create(emitter -> {
            Deferred<ArrayList<KeyValue>> caller = client.get(get);
            EventEmitter.bindMono(emitter, caller);
        });
    }

    public Mono<Void> put(final PutRequest put) {

        return Mono.create(emitter -> {
            Deferred<Object> caller = client.put(put);
            EventEmitter.bindEmptyMono(emitter, caller);
        });
    }

    public Mono<Void> delete(final DeleteRequest delete) {

        return Mono.create(emitter -> {
            Deferred<Object> caller = client.delete(delete);
            EventEmitter.bindEmptyMono(emitter, caller);
        });
    }

    public Mono<Void> shutdown() {

        return Mono.create(emitter -> {
            Deferred<Object> caller = client.shutdown();
            EventEmitter.bindEmptyMono(emitter, caller);
        });
    }

    public Mono<Void> flush() {

        return Mono.create(emitter -> {
            Deferred<Object> caller = client.flush();
            EventEmitter.bindEmptyMono(emitter, caller);
        });
    }

    public RxHBaseScanner newScanner(final byte[] table) {

        return RxHBaseScanner.from(this.client.newScanner(table));
    }

    public RxHBaseScanner newScanner(final String table) {

        return newScanner(table.getBytes(StandardCharsets.UTF_8));
    }

}
