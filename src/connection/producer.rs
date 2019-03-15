extern crate futures;
extern crate rdkafka;

use futures::*;
use std::option;

use rdkafka::client::EmptyContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

pub struct Producer {
    producer: FutureProducer<EmptyContext>,
    topic: String,
    key: u32,
}

impl Producer {
    pub fn new(topic: String) -> Producer {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer<_>>()
            .expect("Producer creation error");
        Producer {
            producer : producer,
            topic : topic,
            key : 0u32,
        }
    }

    pub fn write(&mut self, value: String) {
        self.producer
            .send_copy(&self.topic, None, Some(&value), Some(&self.key.to_string()), None, 0)
            .map(move |delivery_status| {
                println!("Delivery status for message {} received", value);
                delivery_status
            });
        self.key += 1;
    }
}
