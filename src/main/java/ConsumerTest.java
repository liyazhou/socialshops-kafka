import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerTest extends Thread {

    private final KafkaConsumer<String, String> consumer;

    public ConsumerTest() {
        Properties props = new Properties();
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("kafka-consumer.properties");
        try {
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
//            default props
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        this.consumer = new KafkaConsumer(props);


    }

    @Override
    public void run() {
        this.consumer.subscribe(Arrays.asList("my_test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("receive:" + record.value());
            }
        }
    }
}