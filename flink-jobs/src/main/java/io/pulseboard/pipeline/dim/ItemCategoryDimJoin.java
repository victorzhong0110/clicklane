package io.pulseboard.pipeline.dim;

import io.pulseboard.pipeline.model.DimItem;
import io.pulseboard.pipeline.model.Event;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 商品 -> 类目。
 *
 * <p>维表是这次回放的静态快照，在 open() 里读进 HashMap。事件先到、维表还没到的
 * 广播 join 竞态在这个场景里没有收益，反而会把没关联上的商品误记成 UNKNOWN。
 * 维表如果会持续变化，应该换成广播状态或 Flink SQL 的 temporal join，见 design.md。
 */
public class ItemCategoryDimJoin extends RichMapFunction<Event, Event> {
    private final String path;
    private final HashMap<Long, DimItem> inline;
    private transient Map<Long, DimItem> dim;

    public ItemCategoryDimJoin(String path) {
        this.path = path;
        this.inline = null;
    }

    public ItemCategoryDimJoin(Map<Long, DimItem> inline) {
        this.path = null;
        this.inline = new HashMap<>(inline);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (inline != null) {
            dim = inline;
            return;
        }
        dim = DimFiles.load(path);
    }

    @Override
    public Event map(Event event) throws IOException {
        if (event.isKick()) {
            return event;
        }
        DimItem item = dim.get(event.itemId);
        if (item == null) {
            event.categoryId = -1L;
            event.categoryCode = "UNKNOWN";
            event.dimMiss = true;
        } else {
            event.categoryId = item.categoryId;
            event.categoryCode = item.categoryCode;
            event.dimMiss = "UNKNOWN".equals(item.categoryCode);
        }
        return event;
    }
}
