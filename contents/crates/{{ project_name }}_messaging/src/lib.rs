pub mod settings;

use anyhow::Result;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use settings::MessagingSettings;
use std::time::Duration;

#[derive(Clone)]
pub struct MessagingClient {
    producer: FutureProducer,
    settings: MessagingSettings,
}

impl MessagingClient {
    pub async fn connect(settings: &MessagingSettings) -> Result<Self> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &settings.brokers);
        config.set("message.timeout.ms", "5000");

        // SASL/PLAIN auth — injected by PAO from the Kafka connection secret
        // (MESSAGING_USERNAME, MESSAGING_PASSWORD, MESSAGING_SASL_MECHANISM).
        if let (Some(username), Some(password)) =
            (settings.username.as_deref(), settings.password.as_deref())
        {
            let mechanism = settings
                .sasl_mechanism
                .as_deref()
                .unwrap_or("PLAIN");
            config.set("security.protocol", "SASL_SSL");
            config.set("sasl.mechanism", mechanism);
            config.set("sasl.username", username);
            config.set("sasl.password", password);
        }

        let producer: FutureProducer = config.create()?;

        tracing::info!(brokers = %settings.brokers, topic = %settings.topic, "Kafka producer connected");

        Ok(Self {
            producer,
            settings: settings.clone(),
        })
    }

    pub fn topic(&self) -> &str {
        &self.settings.topic
    }

    pub async fn publish(&self, topic: &str, key: &str, payload: &[u8]) -> Result<()> {
        self.producer
            .send(
                FutureRecord::to(topic).key(key).payload(payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {e}"))?;
        Ok(())
    }
}
