# MessageStream Library

This library provides an implementation of a `MessageStream` which is designed to asynchronously retrieve messages from a message broker using the Iggy client. It includes functionality for polling messages at a specified interval and managing message buffers.

## Features

- Asynchronous message retrieval
- Configurable polling interval and batch size
- Automatic message buffering

## Usage

### Creating a MessageStream

To create a `MessageStream`, you can use the `new` method or the `default` method.

```rust
use iggy::client::MessageClient;
use iggy::clients::client::IggyClient;
use iggy::identifier::Identifier;
use std::sync::Arc;
use std::time::Duration;

let client = Arc::new(IggyClient::new());
let stream_id = Identifier::new();
let topic_id = Identifier::new();

// Using the new method
let message_stream = MessageStream::new(
    client.clone(),
    stream_id.clone(),
    topic_id.clone(),
    Some(1),
    Consumer::default(),
    Duration::from_millis(500),
    10,
);

// Using the default method
let default_message_stream = MessageStream::default(client, stream_id, topic_id);
```