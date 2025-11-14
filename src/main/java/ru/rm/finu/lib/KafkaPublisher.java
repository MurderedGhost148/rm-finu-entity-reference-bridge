package ru.rm.finu.lib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.rm.finu.config.AppConfig;

import java.io.Serializable;
import java.util.Properties;

@Slf4j
public class KafkaPublisher {
    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<String, String> producer;

    public KafkaPublisher() {
        AppConfig config = AppConfig.load();
        Properties props = new Properties();

        props.put("bootstrap.servers", config.getKafka().getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }

    public void send(String topic, Serializable id, Object object) throws JsonProcessingException {
        String message = mapper.writeValueAsString(object);

        producer.send(new ProducerRecord<>(topic, String.valueOf(id), message));

        log.debug("Sent to Kafka: topic={}, key={}, message={}", topic, id, message);
    }
}
