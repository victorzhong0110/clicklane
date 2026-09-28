package io.pulseboard.pipeline.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 把未提交的行按 checkpoint 编号暂存。
 *
 * <p>提交不在 {@code snapshot} 里发生。调用方要等 Flink 通知 checkpoint 完成，再取走
 * {@code checkpointId} 及更早的批次。被取消的 checkpoint 把行放回开缓冲区，下一次成功的
 * checkpoint 会用新的编号再带走。这样崩溃前没有完成的 checkpoint 不会留下半截写入。
 */
public final class CheckpointCommitBuffer<T> {
    private final List<T> open = new ArrayList<>();
    private final TreeMap<Long, List<T>> pending = new TreeMap<>();

    public void add(T value) {
        open.add(value);
    }

    public int openCount() {
        return open.size();
    }

    public List<T> snapshot(long checkpointId) {
        if (pending.containsKey(checkpointId)) {
            throw new IllegalStateException("checkpoint already snapshotted: " + checkpointId);
        }
        List<T> batch = new ArrayList<>(open);
        pending.put(checkpointId, batch);
        open.clear();
        return batch;
    }

    /** 取走编号小于等于 checkpointId 的批次，按编号升序。 */
    public List<PendingBatch<T>> completeThrough(long checkpointId) {
        List<PendingBatch<T>> ready = new ArrayList<>();
        var iterator = pending.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<T>> entry = iterator.next();
            if (entry.getKey() > checkpointId) {
                break;
            }
            ready.add(new PendingBatch<>(entry.getKey(), entry.getValue()));
            iterator.remove();
        }
        return ready;
    }

    /**
     * checkpoint 被取消时，这批行还没有对齐到任何一次成功的快照。
     * 放回开缓冲区的前面，跟取消之后新到的行一起进入下一次快照。
     */
    public void abort(long checkpointId) {
        List<T> batch = pending.remove(checkpointId);
        if (batch == null || batch.isEmpty()) {
            return;
        }
        List<T> merged = new ArrayList<>(batch.size() + open.size());
        merged.addAll(batch);
        merged.addAll(open);
        open.clear();
        open.addAll(merged);
    }

    public boolean hasPending(long checkpointId) {
        return pending.containsKey(checkpointId);
    }

    public static final class PendingBatch<T> {
        public final long checkpointId;
        public final List<T> rows;

        PendingBatch(long checkpointId, List<T> rows) {
            this.checkpointId = checkpointId;
            this.rows = rows;
        }
    }
}
