import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by liyazhou on 2017/2/26.
 */
public class MyTopicProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        InputStream in = ClassLoader.getSystemResourceAsStream("kafka-producer.properties");
        try {
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
//        props.put("bootstrap.servers", "127.0.0.1:9091");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        } finally {
            IOUtils.closeQuietly(in);
        }

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));

        producer.close();

        Producer<byte[], byte[]> producer1 = new KafkaProducer<byte[], byte[]>(props);
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value);
        try {
            producer1.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        ProducerRecord<byte[],byte[]> myRecord = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
        producer1.send(myRecord,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e != null)
                            e.printStackTrace();
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                    }
                });

//        发送到同一个分区的消息回调保证按一定的顺序执行，也就是说，在下面的例子中 callback1 保证执行 callback2 之前：
//        producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
//        producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
    }
}
