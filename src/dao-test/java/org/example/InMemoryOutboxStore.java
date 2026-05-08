package org.example;

import io.kiw.luxis.web.messaging.OutboxEvent;
import io.kiw.luxis.web.messaging.OutboxStore;
import io.kiw.luxis.web.messaging.PendingOutboxEvent;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

final class InMemoryOutboxStore implements OutboxStore<InMemoryDatabaseClient.Tx> {

    private final AtomicLong idSeq = new AtomicLong(0);
    private final List<PendingOutboxEvent> pending = new ArrayList<>();
    private final Set<Long> sent = new HashSet<>();

    @Override
    public synchronized Future<Void> append(final InMemoryDatabaseClient.Tx tx, final List<OutboxEvent> events) {
        for (final OutboxEvent event : events) {
            pending.add(new PendingOutboxEvent(idSeq.incrementAndGet(), event));
        }
        return Future.succeededFuture();
    }

    @Override
    public synchronized Future<List<PendingOutboxEvent>> readPending(final int limit) {
        final List<PendingOutboxEvent> out = new ArrayList<>();
        for (final PendingOutboxEvent pe : pending) {
            if (sent.contains(pe.id())) {
                continue;
            }
            out.add(pe);
            if (out.size() >= limit) {
                break;
            }
        }
        return Future.succeededFuture(out);
    }

    @Override
    public synchronized Future<Void> markBatchSent(final List<Long> ids) {
        sent.addAll(ids);
        pending.removeIf(pe -> sent.contains(pe.id()));
        return Future.succeededFuture();
    }

    @Override
    public long pollIntervalMillis() {
        return 50L;
    }
}
