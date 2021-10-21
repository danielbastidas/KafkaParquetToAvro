package com.blueparrotriot;

import com.blueparrotriot.avro.student.Student;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ParquetToAvroProducer {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetToAvroProducer.class);
    private static final String TOPIC = "parquet-source";

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer<String, Student> producer = new KafkaProducer<>(props);

        Thread shutdownHook = new Thread(producer::close);
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        Parquet parquetData = ParquetReaderUtils.getParquetData(args[0]);

        int count = 0;
        for (SimpleGroup student: parquetData.getData()) {

            int cid = student.getInteger("cid", 0);
            Student value = serializeStudentInformation(student);
            LOG.info("["+(++count)+"] Sending to Kafka on the " + TOPIC +
                    " topic the " +
                    "following message: \n" + student.toString());

            ProducerRecord<String, Student> producerRecord =
                    new ProducerRecord<>(TOPIC, String.valueOf(cid), value);
            producer.send(producerRecord);
            producer.flush();
        }

        System.out.printf("%d records were sended to the topic %s", count, TOPIC);
    }

    public static Student serializeStudentInformation(SimpleGroup student) throws IOException {
        return Student.newBuilder()
                .setId(student.getDouble("id",0))
                .setFemale(student.getString("female",0))
                .setSes(student.getString("ses",0))
                .setSchtyp(student.getString("schtyp", 0))
                .setProg(student.getString("prog",0))
                .setRead(student.getDouble("read", 0))
                .setWrite(student.getDouble("write",0))
                .setMath(student.getDouble("math",0))
                .setScience(student.getDouble("science",0))
                .setSocst(student.getDouble("socst",0))
                .setHonors(student.getString("honors", 0))
                .setAwards(student.getDouble("awards", 0))
                .setCid(student.getInteger("cid", 0))
                .build();
    }

}
