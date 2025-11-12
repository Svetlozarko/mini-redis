    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::{RwLock, mpsc};
    use regex::Regex;

    pub type PubSubManager = Arc<RwLock<PubSubState>>;

    #[derive(Debug, Clone)]
    pub enum PubSubMessage {
        Message { channel: String, message: String },
        Subscribe { channel: String, count: usize },
        Unsubscribe { channel: String, count: usize },
        PSubscribe { pattern: String, count: usize },
        PUnsubscribe { pattern: String, count: usize },
    }

    pub struct PubSubState {
        // Channel -> Set of subscriber IDs
        pub channels: HashMap<String, HashSet<usize>>,
        // Pattern -> Set of subscriber IDs
        pub patterns: HashMap<String, HashSet<usize>>,
        // Subscriber ID -> Sender channel
        pub subscribers: HashMap<usize, mpsc::UnboundedSender<PubSubMessage>>,
        next_subscriber_id: usize,
    }

    impl PubSubState {
        pub fn new() -> Self {
            Self {
                channels: HashMap::new(),
                patterns: HashMap::new(),
                subscribers: HashMap::new(),
                next_subscriber_id: 1,
            }
        }

        pub fn create_subscriber(&mut self) -> (usize, mpsc::UnboundedReceiver<PubSubMessage>) {
            let id = self.next_subscriber_id;
            self.next_subscriber_id += 1;

            let (tx, rx) = mpsc::unbounded_channel();
            self.subscribers.insert(id, tx);

            (id, rx)
        }

        pub fn remove_subscriber(&mut self, subscriber_id: usize) {
            self.subscribers.remove(&subscriber_id);

            // Remove from all channels
            for subscribers in self.channels.values_mut() {
                subscribers.remove(&subscriber_id);
            }

            // Remove from all patterns
            for subscribers in self.patterns.values_mut() {
                subscribers.remove(&subscriber_id);
            }

            // Clean up empty channels and patterns
            self.channels.retain(|_, subs| !subs.is_empty());
            self.patterns.retain(|_, subs| !subs.is_empty());
        }

        pub fn subscribe(&mut self, subscriber_id: usize, channel: String) -> usize {
            self.channels
                .entry(channel.clone())
                .or_insert_with(HashSet::new)
                .insert(subscriber_id);

            self.get_subscription_count(subscriber_id)
        }

        pub fn unsubscribe(&mut self, subscriber_id: usize, channel: &str) -> usize {
            if let Some(subscribers) = self.channels.get_mut(channel) {
                subscribers.remove(&subscriber_id);
                if subscribers.is_empty() {
                    self.channels.remove(channel);
                }
            }

            self.get_subscription_count(subscriber_id)
        }

        pub fn psubscribe(&mut self, subscriber_id: usize, pattern: String) -> usize {
            self.patterns
                .entry(pattern.clone())
                .or_insert_with(HashSet::new)
                .insert(subscriber_id);

            self.get_subscription_count(subscriber_id)
        }

        pub fn punsubscribe(&mut self, subscriber_id: usize, pattern: &str) -> usize {
            if let Some(subscribers) = self.patterns.get_mut(pattern) {
                subscribers.remove(&subscriber_id);
                if subscribers.is_empty() {
                    self.patterns.remove(pattern);
                }
            }

            self.get_subscription_count(subscriber_id)
        }

        pub fn publish(&self, channel: &str, message: String) -> usize {
            let mut recipient_count = 0;

            // Send to exact channel subscribers
            if let Some(subscribers) = self.channels.get(channel) {
                for &subscriber_id in subscribers {
                    if let Some(tx) = self.subscribers.get(&subscriber_id) {
                        let _ = tx.send(PubSubMessage::Message {
                            channel: channel.to_string(),
                            message: message.clone(),
                        });
                        recipient_count += 1;
                    }
                }
            }

            // Send to pattern subscribers
            for (pattern, subscribers) in &self.patterns {
                if pattern_matches(pattern, channel) {
                    for &subscriber_id in subscribers {
                        if let Some(tx) = self.subscribers.get(&subscriber_id) {
                            let _ = tx.send(PubSubMessage::Message {
                                channel: channel.to_string(),
                                message: message.clone(),
                            });
                            recipient_count += 1;
                        }
                    }
                }
            }

            recipient_count
        }

        fn get_subscription_count(&self, subscriber_id: usize) -> usize {
            let mut count = 0;

            for subscribers in self.channels.values() {
                if subscribers.contains(&subscriber_id) {
                    count += 1;
                }
            }

            for subscribers in self.patterns.values() {
                if subscribers.contains(&subscriber_id) {
                    count += 1;
                }
            }

            count
        }

        pub fn get_channels(&self) -> Vec<String> {
            self.channels.keys().cloned().collect()
        }

        pub fn get_patterns(&self) -> Vec<String> {
            self.patterns.keys().cloned().collect()
        }

        pub fn get_channel_subscribers(&self, channel: &str) -> usize {
            self.channels.get(channel).map(|s| s.len()).unwrap_or(0)
        }
    }

    // Convert Redis pattern to regex pattern
    // * matches any sequence of characters
    // ? matches exactly one character
    // [abc] matches a, b, or c
    fn pattern_matches(pattern: &str, channel: &str) -> bool {
        let regex_pattern = pattern
            .replace(".", "\\.")
            .replace("*", ".*")
            .replace("?", ".");

        if let Ok(regex) = Regex::new(&format!("^{}$", regex_pattern)) {
            regex.is_match(channel)
        } else {
            false
        }
    }

    pub fn create_pubsub_manager() -> PubSubManager {
        Arc::new(RwLock::new(PubSubState::new()))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_pattern_matching() {
            assert!(pattern_matches("news.*", "news.sports"));
            assert!(pattern_matches("news.*", "news.weather"));
            assert!(!pattern_matches("news.*", "sports.news"));

            assert!(pattern_matches("news.?", "news.a"));
            assert!(!pattern_matches("news.?", "news.ab"));

            assert!(pattern_matches("news*", "news"));
            assert!(pattern_matches("news*", "newsletter"));
        }
    }
