// Copyright 2023 RobustMQ Team
//
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

use super::{cache::CacheManager, subscribe::parse_subscribe};
use crate::subscribe::subscribe_manager::SubscribeManager;
use common_base::{config::broker_mqtt::broker_mqtt_conf, tools::now_second};
use grpc_clients::pool::ClientPool;
use log::info;
use std::{sync::Arc, time::Duration};
use tokio::{select, sync::broadcast, time::sleep};

pub async fn start_parse_subscribe_by_new_topic_thread(
    client_pool: &Arc<ClientPool>,
    metadata_cache: &Arc<CacheManager>,
    subscribe_manager: &Arc<SubscribeManager>,
    stop_send: broadcast::Sender<bool>,
) {
    info!("Subscribe manager thread started successfully.");
    let mut last_update_time: u64 = 0;
    loop {
        let mut stop_rx = stop_send.subscribe();
        select! {
            val = stop_rx.recv() =>{
                if let Ok(flag) = val {
                    if flag {
                        info!("{}","Subscribe manager thread stopped successfully.");
                        break;
                    }
                }
            }
            _ = parse_subscribe_by_new_topic(
                client_pool,
                metadata_cache,
                subscribe_manager,
                last_update_time) =>{
                    last_update_time = now_second();
                    sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

async fn parse_subscribe_by_new_topic(
    client_pool: &Arc<ClientPool>,
    metadata_cache: &Arc<CacheManager>,
    subscribe_manager: &Arc<SubscribeManager>,
    last_update_time: u64,
) {
    let conf = broker_mqtt_conf();
    for (_, topic) in metadata_cache.topic_info.clone() {
        if topic.create_time < last_update_time {
            continue;
        }

        for (_, subscribe) in subscribe_manager.subscribe_list.clone() {
            if subscribe.broker_id != conf.broker_id {
                continue;
            }

            parse_subscribe(
                client_pool,
                subscribe_manager,
                &subscribe.client_id,
                &topic,
                &subscribe.protocol,
                subscribe.pkid,
                &subscribe.filter,
                &subscribe.subscribe_properties,
            )
            .await;
        }
    }
}
