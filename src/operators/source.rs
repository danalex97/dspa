extern crate csv;
extern crate futures;
extern crate rdkafka;
extern crate rdkafka_sys;
extern crate timely;

use crate::dto::common::{Importable, Timestamped, Watermarkable};

use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::scopes::Scope;
use timely::dataflow::Stream;
use timely::Data;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;

use self::rdkafka::consumer::BaseConsumer;
use csv::StringRecord;
use rdkafka::TopicPartitionList;
use std::time::Duration;

pub trait KafkaSource<G: Scope> {
    fn kafka_string_source<D: Importable<D> + Watermarkable + Data + Timestamped>(
        &self,
        topic: String,
        index: usize,
    ) -> Stream<G, D>;
}

impl<G: Scope<Timestamp = usize>> KafkaSource<G> for G {
    fn kafka_string_source<D: Importable<D> + Watermarkable + Data + Timestamped>(
        &self,
        topic: String,
        index: usize,
    ) -> Stream<G, D> {
        // Extract Kafka topic.
        let brokers = "localhost:9092";

        // Create Kafka consumer configuration.
        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("produce.offset.report", "true")
            .set("auto.offset.reset", "earliest")
            .set("group.id", "example")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("bootstrap.servers", &brokers);

        // Create a Kafka consumer.
        let mut topic_partiton_list = TopicPartitionList::new();
        topic_partiton_list.add_partition(&topic, index as i32);
        let consumer: BaseConsumer = consumer_config.create().unwrap();
        consumer.subscribe(&[&topic]).expect("Failed to subscribe");
        consumer
            .assign(&topic_partiton_list)
            .expect("Unable to configure partition correctly");

        source(self, "Source", |mut capability, info| {
            //let mut message_stream = consumer.start();
            let activator = self.activator_for(&info.address[..]);
            move |output| {
                activator.activate();
                for message in consumer.poll(Duration::from_secs(0)) {
                    match message {
                        Err(_) => println!("Error while reading from stream."),
                        Ok(m) => match m.payload_view::<str>() {
                            None => {}
                            Some(Err(_)) => {}
                            Some(Ok(text)) => {
                                // process payload
                                let v: Vec<&str> = text.split("|").collect();
                                let record = StringRecord::from(v);

                                if &record[0] == "Watermark" {
                                    let watermarked = D::from_watermark(&record[1], index);
                                    if watermarked.timestamp() < *capability.time() {
                                        // stuff on kafka from previous runs, we will igore them
                                        // until we arrive at a relevant event
                                    } else {
                                        capability.downgrade(&watermarked.timestamp());
                                        output.session(&capability).give(watermarked);
                                    }
                                } else {
                                    match D::from_record(record) {
                                        Ok(record) => {
                                            output.session(&capability).give(record);
                                        }
                                        Err(_) => {}
                                    }
                                }
                            }
                        },
                    };
                }
            }
        })
    }
}
