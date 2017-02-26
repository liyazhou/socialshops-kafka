import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by liyazhou on 2017/2/26.
 */
public class ProducerTest extends Thread {

    private final KafkaProducer producer;

    public ProducerTest() {
        Properties props = new Properties();
        InputStream in = ClassLoader.getSystemResourceAsStream("kafka-producer.properties");
        try {
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(in);
        }
        this.producer = new KafkaProducer(props);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String messageStr = "Message_" + messageNo;
            System.out.println("Send:" + messageStr);
            producer.send(new ProducerRecord("my_test", messageStr));
            messageNo++;
            try {
                sleep(20);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
