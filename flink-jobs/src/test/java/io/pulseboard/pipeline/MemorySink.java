package io.pulseboard.pipeline;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 把结果写成文本。Flink 本地执行有时会用子类加载器，静态集合跨不回去，文件可以。
 * 一行一个对象，public 字段用 tab 拼出来。
 */
public class MemorySink<T> implements SinkFunction<T> {
    private final String name;

    public MemorySink(String name) {
        this.name = name;
    }

    public static Path dir() {
        return Path.of(System.getProperty("java.io.tmpdir"), "pulseboard-test");
    }

    public static void reset() throws Exception {
        Path dir = dir();
        if (Files.exists(dir)) {
            try (var stream = Files.list(dir)) {
                for (Path file : stream.toList()) {
                    Files.deleteIfExists(file);
                }
            }
        }
        Files.createDirectories(dir);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        StringBuilder line = new StringBuilder();
        for (Field field : value.getClass().getFields()) {
            if (line.length() > 0) {
                line.append('\t');
            }
            line.append(field.getName()).append('=').append(field.get(value));
        }
        line.append('\n');
        Path file = dir().resolve(name + ".txt");
        synchronized (MemorySink.class) {
            Files.writeString(
                    file,
                    line.toString(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        }
    }

    public static List<Map<String, String>> read(String name) throws Exception {
        Path file = dir().resolve(name + ".txt");
        if (!Files.exists(file)) {
            return List.of();
        }
        List<Map<String, String>> rows = new ArrayList<>();
        for (String line : Files.readAllLines(file)) {
            if (line.isBlank()) {
                continue;
            }
            Map<String, String> row = new LinkedHashMap<>();
            for (String part : line.split("\t")) {
                int eq = part.indexOf('=');
                if (eq > 0) {
                    row.put(part.substring(0, eq), part.substring(eq + 1));
                }
            }
            rows.add(row);
        }
        return rows;
    }
}
