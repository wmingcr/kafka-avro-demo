package com.github.simplesteph.kafka.apps.v2;

import com.example.Customer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    private static final String TOPIC = "customer-avro";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        // working on gson,
        String json = "{\"first_name\":\"John\", \"last_name\":\"Doe\", \"age\":34, \"height\":178f, \"weight\":75f, \"phone_number\":\"(123)-456-7890\", \"email\":\"john.doe@gmail.com\"}";
        Gson gson = new GsonBuilder().create();
        Customer customer = gson.fromJson(json , Customer.class);

        System.out.println(customer);

        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);
        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
                TOPIC, customer
        );

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("发送成功 " + metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();

    }
}
