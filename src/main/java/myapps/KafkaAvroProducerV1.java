package myapps;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerV1
{
    public static void main(String[] args)
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        Customer customer = Customer.newBuilder()
                                .setFirstName("Binod")
                                .setLastName("Pandey")
                                .setAge(26)
                                .setHeight(179.1f)
                                .setWeight(62.1f)
                                .setAutomatedEmail(false)
                                .build();
        KafkaProducer<String, Customer> producer = new KafkaProducer<>(properties);
        String topic = "my-topic";
        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);
        producer.send(producerRecord, (recordMetaData, exception) -> {
            if (exception == null)
            {
                System.out.println("Message sent successfully");
                System.out.println(recordMetaData.toString());
            }
            else
            {
                exception.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }
}
