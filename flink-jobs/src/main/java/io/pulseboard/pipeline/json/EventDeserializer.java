package io.pulseboard.pipeline.json;

import io.pulseboard.pipeline.model.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/** Kafka 的 value 是 JSON。失败时返回带 parseError 的事件，而不是让作业失败。 */
public class EventDeserializer implements DeserializationSchema<Event> {
    @Override
    public Event deserialize(byte[] message) {
        return EventJson.parse(message);
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
