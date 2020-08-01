package org.hbase.reactor;

import com.stumbleupon.async.Deferred;
import lombok.experimental.UtilityClass;
import reactor.core.publisher.MonoSink;

/**
 * @author ajith.km
 */
@UtilityClass
public class EventEmitter {

    public static <T> void bindMono(final MonoSink<T> monoSink, final Deferred<T> deferred) {

        deferred.addCallback(obj -> {
            monoSink.success(obj);
            return obj;
        });
        deferred.addErrback(ex -> {
            monoSink.error((Throwable) ex);
            return ex;
        });
    }

    public static <T> void bindEmptyMono(final MonoSink<Void> voidSink, final Deferred<T> deferred) {

        deferred.addCallback(obj -> {
            voidSink.success();
            return obj;
        });
        deferred.addErrback(ex -> {
            voidSink.error((Throwable) ex);
            return ex;
        });
    }
}
