use iggy::client::MessageClient;
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::models::messages::PolledMessage;
use log::info;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub struct MessageStream {
    client: Arc<IggyClient>,
    stream_id: Identifier,
    topic_id: Identifier,
    partition: Option<u32>,
    consumer: Consumer,
    messages_per_batch: u32,
    polling_interval: Duration,
    buffer: VecDeque<PolledMessage>,
    _marker: PhantomData<PolledMessage>,
}

impl MessageStream {
    pub fn new(
        client: Arc<IggyClient>,
        stream_id: Identifier,
        topic_id: Identifier,
        partition: Option<u32>,
        consumer: Consumer,
        polling_interval: Duration,
        messages_per_batch: u32,
    ) -> Self {
        Self {
            client,
            stream_id,
            topic_id,
            partition,
            consumer,
            messages_per_batch,
            polling_interval,
            buffer: VecDeque::new(),
            _marker: PhantomData,
        }
    }

    /// Creates a new MessageStream with default values.
    /// Defaults to polling every 500ms with 10 messages per batch.
    pub fn default(client: Arc<IggyClient>, stream_id: Identifier, topic_id: Identifier) -> Self {
        Self::new(
            client,
            stream_id,
            topic_id,
            Some(1),
            Consumer::default(),
            Duration::from_millis(500),
            10,
        )
    }

    /// Asynchronously retrieves the next message.
    ///
    /// If no messages are currently available, the stream waits for the polling interval.
    /// Returns `Ok(Some(message))` when a message is available, or `Ok(None)` if no message was
    /// received in a given polling round.
    pub async fn next(&mut self) -> Result<Option<PolledMessage>, IggyError> {
        // If our buffer is empty, poll for more messages.
        if self.buffer.is_empty() {
            info!(
                "Polling messages from stream: {}, topic: {}",
                self.stream_id, self.topic_id
            );
            let polled: iggy::models::messages::PolledMessages = self
                .client
                .poll_messages(
                    &self.stream_id,
                    &self.topic_id,
                    self.partition,
                    &self.consumer,
                    &PollingStrategy::next(),
                    self.messages_per_batch,
                    true,
                )
                .await?;

            if polled.messages.is_empty() {
                info!(
                    "No messages found; sleeping for {} ms.",
                    self.polling_interval.as_millis()
                );
                sleep(self.polling_interval).await;
                return Ok(None);
            }
            // Insert new messages into the buffer (okay to overwrite since it was empty)
            self.buffer = polled.messages.into();
        }

        // SAFETY: Buffer is non-empty
        let next_message = self.buffer.pop_front().unwrap();

        Ok(Some(next_message))
    }
}
