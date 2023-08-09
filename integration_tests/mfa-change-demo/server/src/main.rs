use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::{App, Arg, ArgMatches};

use crate::kafka::KafkaSink;
use crate::recwave::Recwave;
use crate::server::server_server::ServerServer;

mod kafka;
mod model;
mod recwave;
mod server;
mod serving;

#[tokio::main]
async fn main() {
    println!("Reading args");
    let args = get_args();
    let kafka_sink = KafkaSink::new(
        args.value_of("brokers")
            .expect("failed to decode brokers")
            .to_string(),
        args.value_of("output-topic")
            .expect("failed to decode output_topics")
            .to_string(),
    );

    println!("Testing Kafka payload");
    tokio::spawn(KafkaSink::mock_consume());
    kafka_sink
        .send("0".to_string(), "{init: true}".to_string())
        .await;
    let server = ServerServer::new(Recwave { kafka: kafka_sink });

    tonic::transport::Server::builder()
        .add_service(server)
        .serve(SocketAddr::new(
            IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
            2666,
        ))
        .await
        .unwrap()
}

fn get_args<'a>() -> ArgMatches<'a> {
    App::new("recwave-recommender")
        .about("The recommender of recwave")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Kafka broker list")
                .takes_value(true)
                .default_value("localhost:29092"),
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topics")
                .help("Output topics names")
                .default_value("recwave")
                .takes_value(true),
        )
        .get_matches()
}
