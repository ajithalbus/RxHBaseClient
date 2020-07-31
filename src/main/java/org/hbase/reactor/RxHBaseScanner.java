package org.hbase.reactor;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * @author ajith.km
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE, staticName = "from")
public class RxHBaseScanner {

    private final Scanner scanner;

    public Flux<List<KeyValue>> scan() {

        throw new UnsupportedOperationException("This method is not implemented!");
    }
}
