package com.github.simplesteph.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
    public static void main(final String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // Criar propriedades do Producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Criar o producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {

            // Criar os dados do Produtor
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world!");

            // Enviar os dados - assincrono
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Recebido novo Metadado! \n" + "Topico: " + metadata.topic() + "\n" + "Partição: "
                                + metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: "
                                + metadata.timestamp());
                    } else {
                        logger.error("Error no Producer", exception);
                    }
                }

            });
        }
        // por ser assincrono
        producer.flush();
        producer.close();
    }
}