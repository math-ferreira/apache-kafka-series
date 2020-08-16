package com.github.simplesteph.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        /**
         * Set up your blocking queues: Be sure to size these properly based on expected
         * TPS of your stream
         */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // Criar Twitter Client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        // Criar um Producer Kafka
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Adicionar um desligamento
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Parando aplicação");
            logger.info("Desligando Cleint do Twitter");
            client.stop();
            logger.info("Close Producer");
            producer.close();
            logger.info("Finalizado!");
        }));

        // Loop para enviar tweets para o Kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.info("Error: " + e.getMessage());
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                       if (exception != null){  
                           logger.info("Error: " + exception.getMessage());
                       }

                    }
                    
                });
            }
        }
        logger.info("End of application");
    }

    String consumerKey = "Cg202Chc6PAwrxr0mrLOgBqop";
    String consumerSecret = "3W6aM3AOooOWQDdd9hypfkMmDEfRTA0CoRdDu5zoGiJc9RQKQ1";
    String token = "1294659075595218946-YRCUTBtPg7xFgz5qgjt2u8uXkcOFvf";
    String secret = "pBFXjAzH7Ayc7qDNPSN25CxfxZjwR9qNFM9MRNj9g9HIP";

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /**
         * Declare the host you want to connect to, the endpoint, and authentication
         * (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer(){
                // Criar propriedades do Producer
                Properties properties = new Properties();
                properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                // Criar um Safe Producer
                properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
                properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
                properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
                properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

                // Alto Throughput para o producer - alto rendimento, compactando o envio das mensagens
                properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
                properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
                properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        
                // Criar o producer
                KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
                return producer;
    }
}