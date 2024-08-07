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


use crate::storage::{
    mqtt::{
        lastwill::MQTTLastWillStorage, session::MQTTSessionStorage, topic::MQTTTopicStorage,
        user::MQTTUserStorage,
    },
    rocksdb::RocksDBEngine,
};
use common_base::errors::RobustMQError;
use metadata_struct::mqtt::session::MQTTSession;
use prost::Message as _;
use protocol::placement_center::generate::mqtt::{
    CreateSessionRequest, CreateTopicRequest, CreateUserRequest, DeleteSessionRequest,
    DeleteTopicRequest, DeleteUserRequest, SaveLastWillMessageRequest,
    SetTopicRetainMessageRequest, UpdateSessionRequest,
};
use std::sync::Arc;
use tonic::Status;

pub struct DataRouteMQTT {
    pub rocksdb_engine_handler: Arc<RocksDBEngine>,
}
impl DataRouteMQTT {
    pub fn new(rocksdb_engine_handler: Arc<RocksDBEngine>) -> Self {
        return DataRouteMQTT {
            rocksdb_engine_handler,
        };
    }

    pub fn create_user(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req = CreateUserRequest::decode(value.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))
            .unwrap();
        let storage = MQTTUserStorage::new(self.rocksdb_engine_handler.clone());
        match storage.save(&req.cluster_name, &req.user_name, req.content) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn delete_user(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req = DeleteUserRequest::decode(value.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))
            .unwrap();
        let storage = MQTTUserStorage::new(self.rocksdb_engine_handler.clone());
        match storage.delete(&req.cluster_name, &req.user_name) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn create_topic(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req = CreateTopicRequest::decode(value.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))
            .unwrap();
        let storage = MQTTTopicStorage::new(self.rocksdb_engine_handler.clone());

        match storage.save(&req.cluster_name, &req.topic_name, req.content) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn delete_topic(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req = DeleteTopicRequest::decode(value.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))
            .unwrap();
        let storage = MQTTTopicStorage::new(self.rocksdb_engine_handler.clone());
        match storage.delete(&req.cluster_name, &req.topic_name) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn set_topic_retain_message(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req: SetTopicRetainMessageRequest =
            SetTopicRetainMessageRequest::decode(value.as_ref())
                .map_err(|e| Status::invalid_argument(e.to_string()))
                .unwrap();
        let storage = MQTTTopicStorage::new(self.rocksdb_engine_handler.clone());
        match storage.set_topic_retain_message(
            &req.cluster_name,
            &req.topic_name,
            req.retain_message,
            req.retain_message_expired_at
        ) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn save_last_will_message(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req = SaveLastWillMessageRequest::decode(value.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))
            .unwrap();
        let storage = MQTTLastWillStorage::new(self.rocksdb_engine_handler.clone());
        match storage.save(&req.cluster_name, &req.client_id, req.last_will_message) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn create_session(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req = CreateSessionRequest::decode(value.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))
            .unwrap();
        let storage = MQTTSessionStorage::new(self.rocksdb_engine_handler.clone());

        match storage.save(&req.cluster_name, &req.client_id, req.session) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn update_session(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req = UpdateSessionRequest::decode(value.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))
            .unwrap();
        let storage = MQTTSessionStorage::new(self.rocksdb_engine_handler.clone());
        let result = match storage.list(&req.cluster_name, Some(req.client_id.clone())) {
            Ok(data) => {
                if data.len() == 0 {
                    return Err(RobustMQError::SessionDoesNotExist);
                }
                data
            }
            Err(e) => {
                return Err(e);
            }
        };
        let session = result.get(0).unwrap();
        let mut session = serde_json::from_slice::<MQTTSession>(&session.data).unwrap();
        if req.connection_id > 0 {
            session.update_connnction_id(Some(req.connection_id));
        } else {
            session.update_connnction_id(None);
        }

        if req.broker_id > 0 {
            session.update_broker_id(Some(req.broker_id));
        } else {
            session.update_broker_id(None);
        }

        if req.reconnect_time > 0 {
            session.reconnect_time = Some(req.reconnect_time);
        }

        if req.distinct_time > 0 {
            session.distinct_time = Some(req.distinct_time);
        } else {
            session.distinct_time = None;
        }

        match storage.save(&req.cluster_name, &req.client_id, session.encode()) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn delete_session(&self, value: Vec<u8>) -> Result<(), RobustMQError> {
        let req = DeleteSessionRequest::decode(value.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))
            .unwrap();
        let storage = MQTTSessionStorage::new(self.rocksdb_engine_handler.clone());
        match storage.delete(&req.cluster_name, &req.client_id) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}
