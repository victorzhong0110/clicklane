package io.pulseboard.pipeline.dq;

import io.pulseboard.pipeline.model.Event;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 按 naturalKey 记“见过没有”。
 *
 * <p>默认仍把重复事件放到主输出：PV 的业务定义是浏览次数，1 秒精度下无法区分
 * “真重复行”和“同一秒点了两次”。重复次数写到侧输出，供质量表统计。
 * 如果业务改成按唯一交互计，把 forwardDuplicates 设成 false 即可。
 *
 * <p>状态加了 2 小时处理时间 TTL。回放只有几分钟，TTL 不会误伤；
 * 长时间跑的时候用它挡住状态无限增长，代价是 TTL 之外的重复会漏检。
 */
public class DedupFunction extends KeyedProcessFunction<String, Event, Event> {
    public static final OutputTag<Event> DUPLICATES = new OutputTag<Event>("duplicate-events") {};

    private final boolean forwardDuplicates;
    private transient ValueState<Boolean> seen;

    public DedupFunction(boolean forwardDuplicates) {
        this.forwardDuplicates = forwardDuplicates;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seen", Boolean.class);
        desc.enableTimeToLive(ttl);
        seen = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
        if (event.isKick()) {
            out.collect(event);
            return;
        }
        if (Boolean.TRUE.equals(seen.value())) {
            ctx.output(DUPLICATES, event);
            if (forwardDuplicates) {
                out.collect(event);
            }
            return;
        }
        seen.update(Boolean.TRUE);
        out.collect(event);
    }
}
