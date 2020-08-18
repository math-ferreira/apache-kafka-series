package com.github.simplesteph.kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
    public static void main(final String[] args) {
        
        // Criar propriedades do Producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Criar o producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Criar os dados do Produtor
        ProducerRecord<String, String> record = new ProducerRecord<String,String>("first_topic", "hello world!");

        // Enviar os dados - assincrono
        producer.send(record);

        // por ser assincrono
        producer.flush();
        producer.close();
    }
} 