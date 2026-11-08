package ru.mai.bigdataflink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class StarStreamingJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "mock_data");
        String groupId = System.getenv().getOrDefault(
                "KAFKA_GROUP_ID", "flink-star-consumer");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "false")
                .build();

        DataStream<Map<String, String>> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka JSON Source")
                .map(new JsonToMap())
                .name("Parse JSON messages");

        events.addSink(new PostgresStarSink())
                .name("PostgreSQL Star Schema Sink")
                .setParallelism(1);

        env.execute("BigDataFlink: Kafka -> Flink -> PostgreSQL Star Schema");
    }

    private static class JsonToMap implements MapFunction<String, Map<String, String>> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public Map<String, String> map(String json) throws Exception {
            JsonNode root = MAPPER.readTree(json);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("Kafka message must be a JSON object");
            }

            Map<String, String> row = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                JsonNode value = field.getValue();
                row.put(field.getKey(), value == null || value.isNull() ? "" : value.asText());
            }
            return row;
        }
    }
}
