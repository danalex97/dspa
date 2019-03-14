extern crate futures;
extern crate rdkafka;

use futures::*;

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

fn produce(brokers: &str, topic_name: &str) {
    let producer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create::<FutureProducer<_>>()
        .expect("Producer creation error");

    let futures = (0..5)
        .map(|i| {
            let value = format!("Message {}", i);
            let key = format!("Key {}", i);

            producer
                .send_copy(topic_name, None, Some(&value), Some(&key), None, 0)
                .map(move |delivery_status| {
                    println!("Delivery status for message {} received", i);
                    delivery_status
                })
        }).collect::<Vec<_>>();

    for future in futures {
        println!("Future completed. Result: {:?}", future.wait());
    }
}

pub fn producer() {
    let topic = "comments";
    let brokers = "localhost:9092";

    produce(brokers, topic);
}
