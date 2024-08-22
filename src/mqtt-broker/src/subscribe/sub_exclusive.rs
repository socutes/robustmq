// Copyright 2023 RobustMQ Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    handler::{
        cache_manager::{CacheManager, QosAckPackageData, QosAckPackageType, QosAckPacketInfo},
        retain::try_send_retain_message,
    },
    server::{connection_manager::ConnectionManager, packet::ResponsePackage},
    storage::message::MessageStorage,
};
use bytes::Bytes;
use clients::poll::ClientPool;
use common_base::{error::robustmq::RobustMQError, tools::now_second};
use log::{error, info};
use metadata_struct::mqtt::message::MQTTMessage;
use protocol::mqtt::common::{MQTTPacket, MQTTProtocol, Publish, PublishProperties, QoS};
use std::{sync::Arc, time::Duration};
use storage_adapter::storage::StorageAdapter;
use tokio::{
    sync::broadcast::{self},
    time::sleep,
};

use super::{
    sub_common::{
        loop_commit_offset, min_qos, publish_message_qos0, publish_message_to_client,
        qos2_send_publish, qos2_send_pubrel, wait_packet_ack,
    },
    subscribe_manager::SubscribeManager,
};

pub struct SubscribeExclusive<S> {
    cache_manager: Arc<CacheManager>,
    subscribe_manager: Arc<SubscribeManager>,
    connection_manager: Arc<ConnectionManager>,
    client_poll: Arc<ClientPool>,
    message_storage: Arc<S>,
}

impl<S> SubscribeExclusive<S>
where
    S: StorageAdapter + Sync + Send + 'static + Clone,
{
    pub fn new(
        message_storage: Arc<S>,
        cache_manager: Arc<CacheManager>,
        subscribe_manager: Arc<SubscribeManager>,
        connection_manager: Arc<ConnectionManager>,
        client_poll: Arc<ClientPool>,
    ) -> Self {
        return SubscribeExclusive {
            message_storage,
            cache_manager,
            subscribe_manager,
            connection_manager,
            client_poll,
        };
    }

    pub async fn start(&self) {
        loop {
            self.start_push_thread().await;
            self.try_thread_gc().await;
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn try_thread_gc(&self) {
        // Periodically verify that a push task is running, but the subscribe task has stopped
        // If so, stop the process and clean up the data
        for (exclusive_key, sx) in self.subscribe_manager.exclusive_push_thread.clone() {
            if !self
                .subscribe_manager
                .exclusive_subscribe
                .contains_key(&exclusive_key)
            {
                match sx.send(true) {
                    Ok(_) => {
                        self.subscribe_manager
                            .exclusive_push_thread
                            .remove(&exclusive_key);
                    }
                    Err(_) => {}
                }
            }
        }
    }

    // Handles exclusive subscription push tasks
    // Exclusively subscribed messages are pushed directly to the consuming client
    async fn start_push_thread(&self) {
        for (exclusive_key, subscriber) in self.subscribe_manager.exclusive_subscribe.clone() {
            let client_id = subscriber.client_id.clone();

            if self
                .subscribe_manager
                .exclusive_push_thread
                .contains_key(&exclusive_key)
            {
                continue;
            }

            let (sub_thread_stop_sx, mut sub_thread_stop_rx) = broadcast::channel(1);
            let message_storage = self.message_storage.clone();
            let cache_manager = self.cache_manager.clone();
            let connection_manager = self.connection_manager.clone();
            let subscribe_manager = self.subscribe_manager.clone();
            let client_poll = self.client_poll.clone();

            // Subscribe to the data push thread
            self.subscribe_manager
                .exclusive_push_thread
                .insert(exclusive_key.clone(), sub_thread_stop_sx.clone());

            tokio::spawn(async move {
                info!(
                        "Exclusive push thread for client_id [{}],topic_id [{}] was started successfully",
                        client_id, subscriber.topic_id
                    );
                let message_storage = MessageStorage::new(message_storage);
                let group_id = format!("system_sub_{}_{}", client_id, subscriber.topic_id);
                let record_num = 5;
                let max_wait_ms = 100;

                let cluster_qos = cache_manager.get_cluster_info().max_qos();
                let qos = min_qos(cluster_qos, subscriber.qos);

                let mut sub_ids = Vec::new();
                if let Some(id) = subscriber.subscription_identifier {
                    sub_ids.push(id);
                }

                try_send_retain_message(
                    client_id.clone(),
                    subscriber.clone(),
                    client_poll.clone(),
                    cache_manager.clone(),
                    connection_manager.clone(),
                    sub_thread_stop_sx.clone(),
                )
                .await;

                loop {
                    match sub_thread_stop_rx.try_recv() {
                        Ok(flag) => {
                            if flag {
                                info!(
                                        "Exclusive Push thread for client_id [{}],topic_id [{}] was stopped successfully",
                                        client_id.clone(),
                                    subscriber.topic_id
                                    );
                                break;
                            }
                        }
                        Err(_) => {}
                    }
                    match message_storage
                        .read_topic_message(
                            subscriber.topic_id.clone(),
                            group_id.clone(),
                            record_num,
                        )
                        .await
                    {
                        Ok(result) => {
                            if result.len() == 0 {
                                sleep(Duration::from_millis(max_wait_ms)).await;
                                continue;
                            }

                            for record in result.clone() {
                                let msg = match MQTTMessage::decode_record(record.clone()) {
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        error!("Storage layer message Decord failed with error message :{}",e);
                                        match message_storage
                                            .commit_group_offset(
                                                subscriber.topic_id.clone(),
                                                group_id.clone(),
                                                record.offset,
                                            )
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(e) => {
                                                error!("{}", e);
                                            }
                                        }
                                        continue;
                                    }
                                };

                                if subscriber.nolocal && (subscriber.client_id == msg.client_id) {
                                    continue;
                                }

                                let retain = if subscriber.preserve_retain {
                                    msg.retain
                                } else {
                                    false
                                };

                                let mut publish = Publish {
                                    dup: false,
                                    qos,
                                    pkid: 0,
                                    retain,
                                    topic: Bytes::from(subscriber.topic_name.clone()),
                                    payload: Bytes::from(msg.payload),
                                };

                                let properties = PublishProperties {
                                    payload_format_indicator: msg.format_indicator,
                                    message_expiry_interval: msg.expiry_interval,
                                    topic_alias: None,
                                    response_topic: msg.response_topic,
                                    correlation_data: msg.correlation_data,
                                    user_properties: msg.user_properties,
                                    subscription_identifiers: sub_ids.clone(),
                                    content_type: msg.content_type,
                                };

                                match qos {
                                    QoS::AtMostOnce => {
                                        publish_message_qos0(
                                            &cache_manager,
                                            &client_id,
                                            &publish,
                                            &Some(properties),
                                            &connection_manager,
                                            &sub_thread_stop_sx,
                                        )
                                        .await;
                                    }

                                    QoS::AtLeastOnce => {
                                        let pkid: u16 = cache_manager.get_pkid(&client_id).await;
                                        publish.pkid = pkid;

                                        let (wait_puback_sx, _) = broadcast::channel(1);
                                        cache_manager.add_ack_packet(
                                            &client_id,
                                            pkid,
                                            QosAckPacketInfo {
                                                sx: wait_puback_sx.clone(),
                                                create_time: now_second(),
                                            },
                                        );

                                        match exclusive_publish_message_qos1(
                                            &cache_manager,
                                            &client_id,
                                            publish,
                                            &properties,
                                            pkid,
                                            &connection_manager,
                                            &sub_thread_stop_sx,
                                            &wait_puback_sx,
                                        )
                                        .await
                                        {
                                            Ok(()) => {
                                                cache_manager.remove_pkid_info(&client_id, pkid);
                                                cache_manager.remove_ack_packet(&client_id, pkid);
                                            }
                                            Err(e) => {
                                                error!("{}", e);
                                            }
                                        }
                                    }

                                    QoS::ExactlyOnce => {
                                        let pkid: u16 = cache_manager.get_pkid(&client_id).await;
                                        publish.pkid = pkid;

                                        let (wait_ack_sx, _) = broadcast::channel(1);
                                        cache_manager.add_ack_packet(
                                            &client_id,
                                            pkid,
                                            QosAckPacketInfo {
                                                sx: wait_ack_sx.clone(),
                                                create_time: now_second(),
                                            },
                                        );
                                        match exclusive_publish_message_qos2(
                                            &cache_manager,
                                            &client_id,
                                            &publish,
                                            &properties,
                                            pkid,
                                            &connection_manager,
                                            &sub_thread_stop_sx,
                                            &wait_ack_sx,
                                        )
                                        .await
                                        {
                                            Ok(()) => {
                                                cache_manager.remove_pkid_info(&client_id, pkid);
                                                cache_manager.remove_ack_packet(&client_id, pkid);
                                            }
                                            Err(e) => {
                                                error!("{}", e);
                                            }
                                        }
                                    }
                                };

                                // commit offset
                                loop_commit_offset(
                                    &message_storage,
                                    &subscriber.topic_id,
                                    &group_id,
                                    record.offset,
                                )
                                .await;
                                continue;
                            }
                        }
                        Err(e) => {
                            error!(
                                    "Failed to read message from storage, failure message: {},topic:{},group{}",
                                    e.to_string(),
                                    subscriber.topic_id.clone(),
                                    group_id.clone()
                                );
                            sleep(Duration::from_millis(max_wait_ms)).await;
                        }
                    }
                }

                subscribe_manager
                    .exclusive_push_thread
                    .remove(&exclusive_key);
            });
        }
    }
}

// When the subscribed QOS is 1, we need to keep retrying to send the message to the client.
// To avoid messages that are not successfully pushed to the client. When the client Session expires,
// the push thread will exit automatically and will not attempt to push again.
pub async fn exclusive_publish_message_qos1(
    metadata_cache: &Arc<CacheManager>,
    client_id: &String,
    mut publish: Publish,
    publish_properties: &PublishProperties,
    pkid: u16,
    connection_manager: &Arc<ConnectionManager>,
    stop_sx: &broadcast::Sender<bool>,
    wait_puback_sx: &broadcast::Sender<QosAckPackageData>,
) -> Result<(), RobustMQError> {
    let mut retry_times = 0;
    loop {
        match stop_sx.subscribe().try_recv() {
            Ok(flag) => {
                if flag {
                    return Ok(());
                }
            }
            Err(_) => {}
        }

        let connect_id = if let Some(id) = metadata_cache.get_connect_id(&client_id) {
            id
        } else {
            sleep(Duration::from_secs(1)).await;
            continue;
        };

        if let Some(conn) = metadata_cache.get_connection(connect_id) {
            if publish.payload.len() > (conn.max_packet_size as usize) {
                return Ok(());
            }
        }

        retry_times = retry_times + 1;
        publish.dup = retry_times >= 2;

        let mut contain_properties = false;
        if let Some(protocol) = connection_manager.get_connect_protocol(connect_id) {
            if MQTTProtocol::is_mqtt5(&protocol) {
                contain_properties = true;
            }
        }

        let resp = if contain_properties {
            ResponsePackage {
                connection_id: connect_id,
                packet: MQTTPacket::Publish(publish.clone(), Some(publish_properties.clone())),
            }
        } else {
            ResponsePackage {
                connection_id: connect_id,
                packet: MQTTPacket::Publish(publish.clone(), None),
            }
        };

        match publish_message_to_client(resp.clone(), connection_manager).await {
            Ok(_) => {
                if let Some(data) = wait_packet_ack(&wait_puback_sx).await {
                    if data.ack_type == QosAckPackageType::PubAck && data.pkid == pkid {
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to write QOS1 Publish message to response queue, failure message: {}",
                    e
                );
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// send publish message
// wait pubrec message
// send pubrel message
// wait pubcomp message
pub async fn exclusive_publish_message_qos2(
    metadata_cache: &Arc<CacheManager>,
    client_id: &String,
    publish: &Publish,
    publish_properties: &PublishProperties,
    pkid: u16,
    connection_manager: &Arc<ConnectionManager>,
    stop_sx: &broadcast::Sender<bool>,
    wait_ack_sx: &broadcast::Sender<QosAckPackageData>,
) -> Result<(), RobustMQError> {
    // 1. send Publish to Client
    qos2_send_publish(
        connection_manager,
        metadata_cache,
        client_id,
        publish,
        &Some(publish_properties.clone()),
        stop_sx,
    )
    .await?;

    // 2. wait PubRec ack
    loop {
        match stop_sx.subscribe().try_recv() {
            Ok(flag) => {
                if flag {
                    return Ok(());
                }
            }
            Err(_) => {}
        }
        if let Some(data) = wait_packet_ack(wait_ack_sx).await {
            if data.ack_type == QosAckPackageType::PubRec && data.pkid == pkid {
                break;
            }
        } else {
            qos2_send_publish(
                connection_manager,
                metadata_cache,
                client_id,
                publish,
                &Some(publish_properties.clone()),
                stop_sx,
            )
            .await?;
        }
        sleep(Duration::from_millis(1)).await;
    }

    // 3. send PubRel to Client
    qos2_send_pubrel(metadata_cache, client_id, pkid, connection_manager, stop_sx).await;

    // 4. wait pub comp
    loop {
        match stop_sx.subscribe().try_recv() {
            Ok(flag) => {
                if flag {
                    break;
                }
            }
            Err(_) => {}
        }
        if let Some(data) = wait_packet_ack(&wait_ack_sx).await {
            if data.ack_type == QosAckPackageType::PubComp && data.pkid == pkid {
                break;
            }
        } else {
            qos2_send_pubrel(metadata_cache, client_id, pkid, connection_manager, stop_sx).await;
        }
        sleep(Duration::from_millis(1)).await;
    }
    return Ok(());
}
#[cfg(test)]
mod test {}
