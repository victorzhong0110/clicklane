package io.pulseboard.pipeline.sink;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * ClickHouse 写入。
 *
 * <p>明细行（ODS、迟到、重复、非法）的主键和内容都不会变。它们用固定 version {@code 1}
 * 在批次满或 checkpoint 时直接 INSERT。重放写成同一行，{@code ReplacingMergeTree} 折掉。
 * 这些行不进算子状态，避免一次 checkpoint 里堆上几十 MB 的 JSON。
 *
 * <p>聚合行会变。窗口先发出完整结果，任务若在下一次 checkpoint 前死去，恢复后的
 * 部分结果如果用更大的 version 盖上去，FINAL 就会留下更小的 PV。聚合表因此只在
 * checkpoint 完成之后插入，version 用 checkpoint 编号；没完成的缓冲直接丢掉，等 Kafka 重放。
 * 第二阶段每个窗口只发一次，避免恢复后再发一行更小的累加。
 */
public class ClickHouseSink<T> extends RichSinkFunction<T> implements CheckpointedFunction, CheckpointListener {
    static final char FIELD_SEPARATOR = '\u0001';
    static final long IMMUTABLE_VERSION = 1L;

    @FunctionalInterface
    public interface Encoder<T> extends java.io.Serializable {
        String encode(T value, long version);
    }

    private final String endpoint;
    private final String database;
    private final String table;
    private final Encoder<T> encoder;
    private final int batchSize;
    /** 旧的定时刷写间隔。明细行不再靠它提交。 */
    private final long flushMs;
    /** true：聚合表，checkpoint 完成才插入。false：明细表，内容不可变，version 固定为 1。 */
    private final boolean transactional;

    private transient List<T> open;
    private transient CheckpointCommitBuffer<T> staged;
    private transient TreeMap<Long, List<String>> encoded;
    private transient List<String> restoredLines;
    private transient ListState<String> pendingState;
    private transient HttpClient client;

    public ClickHouseSink(
            String endpoint,
            String database,
            String table,
            Encoder<T> encoder,
            int batchSize,
            long flushMs,
            boolean transactional) {
        this.endpoint = endpoint;
        this.database = database;
        this.table = table;
        this.encoder = encoder;
        this.batchSize = batchSize;
        this.flushMs = flushMs;
        this.transactional = transactional;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        open = new ArrayList<>();
        staged = new CheckpointCommitBuffer<>();
        encoded = new TreeMap<>();
        restoredLines = new ArrayList<>();
        if (!transactional) {
            return;
        }
        pendingState = context.getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("ch-staged-" + table, String.class));
        for (String stored : pendingState.get()) {
            restoredLines.add(stored);
        }
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        if (!restoredLines.isEmpty()) {
            insertEncoded(restoredLines);
            restoredLines.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!transactional) {
            flushImmutable();
            return;
        }
        long checkpointId = context.getCheckpointId();
        List<T> batch = staged.snapshot(checkpointId);
        List<String> lines = new ArrayList<>(batch.size());
        for (T value : batch) {
            lines.add(encodeStaged(checkpointId, encoder.encode(value, checkpointId)));
        }
        encoded.put(checkpointId, lines);
        pendingState.clear();
        for (List<String> pending : encoded.values()) {
            for (String line : pending) {
                pendingState.add(line);
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!transactional) {
            return;
        }
        List<CheckpointCommitBuffer.PendingBatch<T>> ready = staged.completeThrough(checkpointId);
        for (CheckpointCommitBuffer.PendingBatch<T> batch : ready) {
            List<String> lines = encoded.get(batch.checkpointId);
            if (lines != null && !lines.isEmpty()) {
                insertEncoded(lines);
            }
            encoded.remove(batch.checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        if (!transactional) {
            return;
        }
        staged.abort(checkpointId);
        encoded.remove(checkpointId);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (transactional) {
            staged.add(value);
            return;
        }
        open.add(value);
        if (open.size() >= Math.max(1, batchSize)) {
            flushImmutable();
        }
    }

    @Override
    public void close() throws Exception {
        if (!transactional && open != null && !open.isEmpty()) {
            flushImmutable();
        }
    }

    private void flushImmutable() throws Exception {
        if (open == null || open.isEmpty()) {
            return;
        }
        List<String> lines = new ArrayList<>(open.size());
        for (T value : open) {
            lines.add(encodeStaged(IMMUTABLE_VERSION, encoder.encode(value, IMMUTABLE_VERSION)));
        }
        open.clear();
        insertEncoded(lines);
    }

    static String encodeStaged(long checkpointId, String json) {
        return checkpointId + String.valueOf(FIELD_SEPARATOR) + json;
    }

    static String jsonOf(String stagedLine) {
        int split = stagedLine.indexOf(FIELD_SEPARATOR);
        if (split < 0) {
            throw new IllegalStateException("staged clickhouse row has no checkpoint id");
        }
        return stagedLine.substring(split + 1);
    }

    private void insertEncoded(List<String> stagedLines) throws Exception {
        List<String> json = new ArrayList<>(stagedLines.size());
        for (String stagedLine : stagedLines) {
            json.add(jsonOf(stagedLine));
        }
        int chunk = Math.max(1, batchSize);
        for (int offset = 0; offset < json.size(); offset += chunk) {
            int end = Math.min(json.size(), offset + chunk);
            post(json.subList(offset, end));
        }
    }

    private void post(List<String> jsonLines) throws Exception {
        if (jsonLines.isEmpty()) {
            return;
        }
        StringBuilder body = new StringBuilder(jsonLines.size() * 128);
        body.append("INSERT INTO ").append(database).append('.').append(table).append(" FORMAT JSONEachRow\n");
        for (String line : jsonLines) {
            body.append(line).append('\n');
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint
                        + "/?database=" + database
                        + "&input_format_defaults_for_omitted_fields=1"))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "text/plain; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            String detail = response.body() == null ? "" : response.body();
            if (detail.length() > 500) {
                detail = detail.substring(0, 500);
            }
            throw new IllegalStateException(
                    "clickhouse insert " + table + " failed: HTTP " + response.statusCode() + " " + detail);
        }
    }

    public long flushMs() {
        return flushMs;
    }

    public boolean transactional() {
        return transactional;
    }
}
