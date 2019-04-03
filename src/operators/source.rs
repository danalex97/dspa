extern crate timely;
extern crate rdkafka;
extern crate kafkaesque;
extern crate csv;

use crate::dto::common::{Importable, Timestamped};

use timely::dataflow::scopes::Scope;
use timely::dataflow::Stream;
use timely::Data;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer, EmptyConsumerContext};
use csv::StringRecord;

const FIXED_BOUNDED_DELAY: usize = 500; //seconds

pub trait KafkaSource<G: Scope> {
    fn kafka_string_source<D : Importable<D> + Data + Timestamped>(&self, topic : String) -> Stream<G, D>;
}

impl<G: Scope<Timestamp=usize>> KafkaSource<G> for G {
    fn kafka_string_source<D : Importable<D> + Data + Timestamped>(&self, topic : String) -> Stream<G, D> {
        // Extract Kafka topic.
        let brokers = "localhost:9092";

        // Create Kafka consumer configuration.
        // Feel free to change parameters here.
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
        let consumer : BaseConsumer<EmptyConsumerContext> = consumer_config.create().expect("Couldn't create consumer");
        consumer.subscribe(&[&topic]).expect("Failed to subscribe to topic");

        kafkaesque::source(self, "KafkaStringSource", consumer, |bytes, capability, output| {
            // If the bytes are utf8, convert to string and send.
            if let Ok(text) = std::str::from_utf8(bytes) {
                let v: Vec<&str> = text.split("|").collect();
                let record = StringRecord::from(v);

                match D::from_record(record) {
                    Ok(record) => {
                        let capability_time = *capability.time();
                        let candidate_time = record.timestamp() - FIXED_BOUNDED_DELAY;

                        // I have a tighter bound for downgrading the capability
                        if capability_time < candidate_time {
                            capability.downgrade(&candidate_time);
                        }

                        output
                            .session(capability)
                            .give(record);
                    },
                    Err(_) => {}
                }
            }

            // Indicate that we are not yet done.
            false
        })
    }
}
