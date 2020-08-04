package org.hbase.reactor;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import reactor.core.publisher.Flux;

import java.util.ArrayList;

/**
 * @author ajith.km
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE, staticName = "from")
public class RxHBaseScanner {

    private final Scanner scanner;

    public Flux<ArrayList<KeyValue>> scan() {

        Flux<ArrayList<KeyValue>> scanFlux = Flux.create(emitter -> {

            EventNotifier<ArrayList<KeyValue>> eventNotifier = EventNotifier.from(scanner::nextRows, emitter::next, emitter::complete, emitter::error);
            emitter.onRequest(eventNotifier::request);
            emitter.onCancel(eventNotifier::cancel);
        });

        return scanFlux.doOnComplete(scanner::close).
                doOnError(err -> scanner.close()).
                doOnCancel(scanner::close);
    }
}
