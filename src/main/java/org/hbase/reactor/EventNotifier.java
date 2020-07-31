package org.hbase.reactor;


import lombok.NoArgsConstructor;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * @author ajith.km
 */
@NoArgsConstructor(staticName = "create")
public class EventNotifier<T> {

    private Collection<Consumer<T>> subscriptions = Sets.newHashSet();

    public void subscribe(Consumer<T> subscription) {
        this.subscriptions.add(subscription);
    }

    public void publish(T event) {
        subscriptions.forEach(subscriber -> subscriber.accept(event));
    }

    public void publish(Collection<T> events) {
        subscriptions.forEach(subscriber -> events.forEach(subscriber::accept));
    }

}
