package io.kiw.luxis.kafka;

import io.kiw.luxis.web.db.BatchUpdateResult;
import io.kiw.luxis.web.db.DatabaseClient;
import io.kiw.luxis.web.db.UpdateResult;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

final class InMemoryDatabaseClient implements DatabaseClient<InMemoryDatabaseClient.Tx, Object, Object> {

    static final class Tx {
    }

    @Override
    public Future<Tx> begin() {
        return Future.succeededFuture(new Tx());
    }

    @Override
    public Future<Void> commit(final Tx tx) {
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> rollback(final Tx tx) {
        return Future.succeededFuture();
    }

    @Override
    public <T> Future<List<T>> query(final Tx tx, final String sql, final Function<Object, T> rowMapper, final Object... params) {
        throw new UnsupportedOperationException("InMemoryDatabaseClient does not implement query");
    }

    @Override
    public <T> Future<List<T>> query(final Tx tx, final String sql, final Function<Object, T> rowMapper, final Map<String, Object> params) {
        throw new UnsupportedOperationException("InMemoryDatabaseClient does not implement query");
    }

    @Override
    public Future<UpdateResult<Object>> update(final Tx tx, final String sql, final Object... params) {
        throw new UnsupportedOperationException("InMemoryDatabaseClient does not implement update");
    }

    @Override
    public Future<UpdateResult<Object>> update(final Tx tx, final String sql, final Map<String, Object> params) {
        throw new UnsupportedOperationException("InMemoryDatabaseClient does not implement update");
    }

    @Override
    public Future<BatchUpdateResult<Object>> updateBatch(final Tx tx, final String sql, final List<Object[]> rows) {
        throw new UnsupportedOperationException("InMemoryDatabaseClient does not implement updateBatch");
    }

    @Override
    public Future<BatchUpdateResult<Object>> updateBatchNamed(final Tx tx, final String sql, final List<Map<String, Object>> rows) {
        throw new UnsupportedOperationException("InMemoryDatabaseClient does not implement updateBatchNamed");
    }
}
