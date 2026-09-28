package io.pulseboard.pipeline.dim;

import io.pulseboard.pipeline.model.DimItem;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** 读取 item_id,category_id,category_code。维表在一次回放里不变，所以作业启动时加载一次。 */
public final class DimFiles implements Serializable {
    private DimFiles() {}

    public static Map<Long, DimItem> load(String path) throws IOException {
        Map<Long, DimItem> dim = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line = reader.readLine();
            if (line == null) {
                return dim;
            }
            if (!line.startsWith("item_id")) {
                parse(dim, line);
            }
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    parse(dim, line);
                }
            }
        }
        return dim;
    }

    private static void parse(Map<Long, DimItem> dim, String line) {
        String[] parts = line.split(",", -1);
        if (parts.length < 3) {
            return;
        }
        long itemId = Long.parseLong(parts[0].trim());
        long categoryId = Long.parseLong(parts[1].trim());
        String code = parts[2].trim();
        dim.put(itemId, new DimItem(itemId, categoryId, code));
    }
}
