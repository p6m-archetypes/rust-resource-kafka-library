use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessagingSettings {
    pub brokers: String,
    pub topic: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub sasl_mechanism: Option<String>,
}

impl Default for MessagingSettings {
    fn default() -> Self {
        Self {
            brokers: std::env::var("MESSAGING_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            topic: std::env::var("MESSAGING_TOPIC")
                .unwrap_or_else(|_| "events".to_string()),
            // PAO injects connection secret keys from the per-app KafkaCredential secret.
            // Secret keys are camelCase (saslMechanism); PAO's sanitize_env_key converts
            // them to UPPER_SNAKE → MESSAGING_SASL_MECHANISM etc.
            username: std::env::var("MESSAGING_USERNAME").ok()
                .filter(|s| !s.is_empty()),
            password: std::env::var("MESSAGING_PASSWORD").ok()
                .filter(|s| !s.is_empty()),
            sasl_mechanism: std::env::var("MESSAGING_SASL_MECHANISM").ok()
                .filter(|s| !s.is_empty()),
        }
    }
}
