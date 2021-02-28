package simpleconsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import io.opentracing.util.GlobalTracer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.protocol.types.Field;
public class SimpleConsumer {
    public static JaegerTracer initTracer(String service) {
        Configuration.SamplerConfiguration samplerConfig = Configuration.SamplerConfiguration.fromEnv().withType("const").withParam(1);
        Configuration.ReporterConfiguration reporterConfig = Configuration.ReporterConfiguration.fromEnv().withLogSpans(true);
        Configuration config = new Configuration(service).withSampler(samplerConfig).withReporter(reporterConfig);
        return config.getTracer();
    }
    public static void main(String[] args) {
        Tracer tracer = initTracer("kafka-tracing");
        GlobalTracer.registerIfAbsent(tracer);
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TracingKafkaConsumer<String, String> tracingConsumer = new TracingKafkaConsumer<>(consumer, tracer);
        tracingConsumer.subscribe(Collections.singletonList("kafka_tutorial"));
        while (true) {
            ConsumerRecords<String, String> records = tracingConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                for (Header header : record.headers()) {
                    System.out.println("header_key = " + header.key() + ", header value = " + new String(header.value()));
                }
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}