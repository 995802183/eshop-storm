package com.wyw.eshop.storm.hot;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AccessLogKafkaSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private ArrayBlockingQueue<String> queue;


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.queue = new ArrayBlockingQueue<>(1000);
        startKafkaConsumer();
    }

    @Override
    public void nextTuple() {
        if(queue.size()>0){
            try {
                String message = queue.take();
                spoutOutputCollector.emit(new Values(message));
            }catch (Exception e){
                e.printStackTrace();
            }
        }else{
            Utils.sleep(100);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    private void startKafkaConsumer(){
        Properties props = new Properties();
        props.put("bootstrap-servers","192.168.74.133:9092");
        props.put("key-deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value-deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id","eshop-storm-group");
        props.put("zookeeper.session.timeout.ms","40000");
        props.put("zookeeper.sync.time.ms","200");
        props.put("auto.commit.interval.ms","1000");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        String topic = "access-log";
        kafkaConsumer.subscribe(Arrays.asList(topic));
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(20);
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    executorService.submit(new KafkaMessageProcessor(record));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private class KafkaMessageProcessor implements Runnable{

        private ConsumerRecord<String,String> record;

        public KafkaMessageProcessor(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void run() {
            String value = record.value();
            try {
                queue.put(value);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
