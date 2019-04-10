extern crate csv;
extern crate rdkafka;
extern crate rdkafka_sys;
extern crate timely;
extern crate futures;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::dto::common::{Importable, Timestamped};

use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::scopes::Scope;
use timely::dataflow::Stream;
use timely::Data;

use rdkafka::message::{Message, Headers};
use rdkafka::client::ClientContext;
use rdkafka::consumer::{Consumer, ConsumerContext, CommitMode, Rebalance};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::util::get_rdkafka_version;
use rdkafka::error::KafkaResult;
use futures::stream::Stream as FutureStream;

use csv::StringRecord;

pub trait KafkaSource<G: Scope> {
    fn kafka_string_source<D: Importable<D> + Data + Timestamped>(
        &self,
        topic: String,
    ) -> Stream<G, D>;
}

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &Rebalance) {}
    fn post_rebalance(&self, _: &Rebalance) {}
    fn commit_callback(&self, _: KafkaResult<()>, _: *mut rdkafka_sys::RDKafkaTopicPartitionList) {}
}

impl<G: Scope<Timestamp = usize>> KafkaSource<G> for G {
    fn kafka_string_source<D: Importable<D> + Data + Timestamped>(
        &self,
        topic: String,
    ) -> Stream<G, D> {
        // Extract Kafka topic.
        let brokers = "localhost:9092";
        let context = CustomContext;

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
        let consumer: StreamConsumer<CustomContext> = consumer_config
            .create_with_context(context)
            .expect("Couldn't create consumer");
        consumer
            .subscribe(&[&topic])
            .expect("Failed to subscribe to topic");

        source(self, "Source", |capability, _| {
            let mut cap = Some(capability);

            move |output| {
                let message_stream = consumer.start();
                for message in message_stream.wait() {
                    match message {
                        Err(_) => println!("Error while reading from stream."),
                        Ok(Err(e)) => println!("Kafka error: {}", e),
                        Ok(Ok(m)) => match m.payload_view::<str>() {
                            None => {},
                            Some(Err(e)) => {},
                            Some(Ok(text)) => {
                                // process payload
                                let v: Vec<&str> = text.split("|").collect();
                                let record = StringRecord::from(v);

                                match D::from_record(record) {
                                    Ok(record) => {
                                        if let Some(cap) = cap.as_mut() {
                                            let capability_time = cap.time().clone();
                                            let candidate_time = record.timestamp() - FIXED_BOUNDED_DELAY;

                                            println!("{:?} {:?}", capability_time, candidate_time);
                                            // I have a tighter bound for downgrading the capability
                                            if capability_time < candidate_time {
                                                cap.downgrade(&candidate_time);
                                            }

                                            output.session(&cap).give(record);
                                        }
                                    }
                                    Err(_) => {}
                                }
                            },
                        },
                    };
                }
            }
        })
    }
}
