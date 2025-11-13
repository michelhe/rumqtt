use rumqttc::{AsyncClient, MqttOptions};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[allow(dead_code)]
mod broker;
use broker::Broker;

#[tokio::test]
async fn test_custom_socket_connector() {
    // Create a custom connector that wraps the default socket_connect
    // This allows us to verify the custom connector path is being used
    let called_flag = Arc::new(AtomicBool::new(false));
    let called_flag_clone = called_flag.clone();

    // Set up MQTT options with custom connector
    let mut options = MqttOptions::new("test-client", "127.0.0.1", 3000);
    options.set_socket_connector(move |host, network_options| {
        let called_flag = called_flag_clone.clone();
        async move {
            called_flag.store(true, std::sync::atomic::Ordering::Relaxed);
            rumqttc::default_socket_connect(host, network_options)
                .await
                .map(|s| Box::new(s) as Box<dyn rumqttc::AsyncReadWrite>)
        }
    });

    // Verify the connector is set
    assert!(options.socket_connector().is_some());

    // Create client and eventloop
    let (_client, mut eventloop) = AsyncClient::new(options, 10);

    // Spawn the event loop in the background so it can start connecting
    let event_handle = tokio::spawn(async move { eventloop.poll().await.unwrap() });

    // Start the broker in the foreground - it will block on accept() until the client connects
    // Once the connection is accepted and the broker completes, we can check the result
    Broker::new(3000, 0, false).await;

    // Wait for the event loop to complete and get the event
    let event = event_handle.await.unwrap();

    // Make sure the custom connector was the one used
    assert!(
        called_flag.load(std::sync::atomic::Ordering::Relaxed),
        "Custom connector should have been called"
    );

    // Verify we got a ConnAck event indicating successful connection
    match event {
        rumqttc::Event::Incoming(rumqttc::Incoming::ConnAck(connack)) => {
            assert_eq!(
                connack.code,
                rumqttc::mqttbytes::v4::ConnectReturnCode::Success
            );
        }
        other => panic!("Expected ConnAck event, got: {:?}", other),
    }
}
