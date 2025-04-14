package io.confluent.csta.timestamp.avro;

import io.confluent.csta.timestamp.Customer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class AvroCompactedProducer {
    private static final Logger log = LoggerFactory.getLogger(AvroCompactedProducer.class);
    private static boolean running = true;
    private static final long SLEEP_TIME_MS = 500;
    private static final String TOPIC = "compacted";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(AvroCompactedProducer.class.getResourceAsStream("/compacted.properties"));

        Producer<String, Customer> producer = new KafkaProducer<>(properties);

        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's set running to false and interrupt thread...");
            mainThread.interrupt();
        }));

        try {
            String firstName="Rui";
            String lastName="Fernandes";
            sendRecord(producer,firstName,lastName);
            firstName="Carmen";
            lastName="Monteiro";
            sendRecord(producer,firstName,lastName);
            firstName="Rui";
            lastName=null;
            sendRecord(producer,firstName,lastName);
            firstName="Carmen";
            lastName="Fernandes";
            sendRecord(producer,firstName,lastName);
        } finally {
            log.info("Closing producer");
            producer.close();
        }

    }

    private static void sendRecord(Producer<String, Customer> producer,String firstName, String lastName) {
        Customer customer = null;
        if(lastName!=null) {
            customer = Customer.newBuilder()
                    .setCustomerTime(Instant.now().truncatedTo(ChronoUnit.MICROS))
                    .setFirstName(firstName)
                    .setLastName(lastName)
                    .build();
        } else {
            customer =null;
        }

        //kafka producer - asynchronous writes
        final ProducerRecord<String, Customer> record = new ProducerRecord<>(TOPIC, firstName, customer);
        //send with callback
        producer.send(record, (metadata, e) -> {
            if (e != null)
                log.info("Send failed for record {}", record, e);
            else
                log.info("Sent name={}, time={} - Partition-{} - Offset {}", record.key(),
                        record.value().getCustomerTime(), metadata.partition(), metadata.offset());
        });
    }
}