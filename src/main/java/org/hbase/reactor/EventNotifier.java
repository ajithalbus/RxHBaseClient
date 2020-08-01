package org.hbase.reactor;


import com.stumbleupon.async.Deferred;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import com.google.common.collect.Queues;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author ajith.km
 */
@RequiredArgsConstructor(staticName = "from")
public class EventNotifier<T> {

    private final Supplier<Deferred<ArrayList<T>>> well;
    private final Consumer<T> sink;
    private final Runnable plug;
    private final Consumer<Throwable> clog;

    private CountDownLatch faucet = new CountDownLatch(1);

    private boolean opened = false;
    private boolean closed = false;
    private boolean killed = false;

    private AtomicLong backlog = new AtomicLong(0);
    private long pumped = 0;

    private Exchanger<Queue<T>> pipe = new Exchanger<>();

    public void cancel() {
        killed = true;
    }

    @SneakyThrows
    private void drain() {

        Queue<T> buffer = Queues.newArrayDeque();

        while (!closed) {
            try {
                do {
                    sink.accept(buffer.remove());
                    pumped++;
                } while (backlog.getAndDecrement() > 0);
            } catch (NoSuchElementException ex) {
                // get the buffer from pump
                buffer = pipe.exchange(buffer);
            }
            if (backlog.get() == 0) {
                // no backlog so stopping discharge for now.
                // Scanner lease could expire meanwhile.
                faucet = new CountDownLatch(1);
                faucet.await();
            }
        }

        // Drain if buffer not empty.
        while (!buffer.isEmpty() && backlog.decrementAndGet() > 0) {
            pumped++;
            sink.accept(buffer.remove());
        }

        // Send the on-complete signal.
        plug.run();
    }

    @SneakyThrows
    private void pump() {

        while (!closed) {

            // latch shall be kicked in result callbacks.
            final CountDownLatch latch = new CountDownLatch(1);

            final Deferred<ArrayList<T>> deferredResult = well.get();

            deferredResult.addCallback(batch -> {

                Queue<T> buffer = Queues.newArrayDeque();

                if (killed || batch == null || batch.isEmpty()) {
                    closed = true;
                    pipe.exchange(buffer);
                    latch.countDown();
                    return null;
                }
                buffer.addAll(batch);
                pipe.exchange(buffer);

                latch.countDown();
                return null;
            });

            deferredResult.addErrback(err -> {

                clog.accept((Throwable) err);
                closed = true;
                latch.countDown();
                return null;
            });

            latch.await();
        }
    }

    public void request(final long ask) {

        open();
        backlog.addAndGet(ask);
        faucet.countDown();
    }

    private void open() {
        if (!opened) {
            new Thread(this::pump, "pumper").start();
            new Thread(this::drain, "drainer").start();
            opened = true;
        }
    }
}


