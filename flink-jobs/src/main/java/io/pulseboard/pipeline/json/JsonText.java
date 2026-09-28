package io.pulseboard.pipeline.json;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** ClickHouse JSONEachRow 用的转义和 UTC 时间格式。不依赖第三方 JSON 库，避免和 Flink 自带的 Jackson 打架。 */
public final class JsonText {
    private static final DateTimeFormatter SECONDS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter MILLIS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private JsonText() {}

    public static String quote(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\':
                    out.append("\\\\");
                    break;
                case '"':
                    out.append("\\\"");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    out.append(ch);
            }
        }
        return out.toString();
    }

    public static String dateTime(long epochMs) {
        return SECONDS.format(Instant.ofEpochMilli(epochMs));
    }

    public static String dateTime64(long epochMs) {
        return MILLIS.format(Instant.ofEpochMilli(epochMs));
    }
}
