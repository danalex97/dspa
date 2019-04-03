extern crate timely;
extern crate rdkafka;
extern crate kafkaesque;

use timely::dataflow::operators::Inspect;
use timely::dataflow::scopes::Scope;
use timely::dataflow::Stream;
use timely::Data;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer, EmptyConsumerContext};

trait KafkaSource<G: Scope> {
    fn kafka_string_source(&self, topic : String) -> Stream<G, String>;
}

impl<G: Scope<Timestamp=usize>> KafkaSource<G> for G {
    fn kafka_string_source(&self, topic : String) -> Stream<G, String> {
        // Extract Kafka topic.
        let brokers = "localhost:9092";

        // Create Kafka consumer configuration.
        // Feel free to change parameters here.
        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("produce.offset.report", "true")
            .set("auto.offset.reset", "smallest")
            .set("group.id", "example")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("bootstrap.servers", &brokers);

        // Create a Kafka consumer.
        let consumer : BaseConsumer<EmptyConsumerContext> = consumer_config.create().expect("Couldn't create consumer");
        consumer.subscribe(&[&topic]).expect("Failed to subscribe to topic");

        kafkaesque::source(self, "KafkaStringSource", consumer, |bytes, capability, output| {

            // If the bytes are utf8, convert to string and send.
            if let Ok(text) = std::str::from_utf8(bytes) {
                output
                    .session(capability)
                    .give(text.to_string());
            }

            // We need some rule to advance timestamps ...
            let time = *capability.time();
            capability.downgrade(&(time + 1));

            // Indicate that we are not yet done.
            false
        })
    }
}
