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

use crate::handler::error::MqttBrokerError;
use crate::storage::connector::ConnectorStorage;
use common_base::tools::now_second;
use common_config::mqtt::broker_mqtt_conf;
use grpc_clients::placement::mqtt::call::placement_list_connector;
use grpc_clients::pool::ClientPool;
use metadata_struct::mqtt::bridge::config_kafka::KafkaConnectorConfig;
use metadata_struct::mqtt::bridge::config_local_file::LocalFileConnectorConfig;
use metadata_struct::mqtt::bridge::connector::MQTTConnector;
use metadata_struct::mqtt::bridge::connector_type::ConnectorType;
use metadata_struct::mqtt::bridge::status::MQTTStatus;
use protocol::broker_mqtt::broker_mqtt_admin::{
    MqttConnectorType, MqttCreateConnectorRequest, MqttDeleteConnectorRequest,
    MqttListConnectorRequest, MqttUpdateConnectorRequest,
};
use protocol::placement_center::placement_center_mqtt::ListConnectorRequest;
use std::sync::Arc;
use tonic::Request;

// List connectors by request
pub async fn list_connector_by_req(
    client_pool: &Arc<ClientPool>,
    request: Request<MqttListConnectorRequest>,
) -> Result<Vec<Vec<u8>>, MqttBrokerError> {
    let req = request.into_inner();
    let config = broker_mqtt_conf();
    let request = ListConnectorRequest {
        cluster_name: config.cluster_name.clone(),
        connector_name: req.connector_name.clone(),
    };

    let connectors = placement_list_connector(client_pool, &config.placement_center, request)
        .await
        .map_err(|e| MqttBrokerError::CommonError(e.to_string()))?
        .connectors;

    Ok(connectors)
}

// Create a new connector
pub async fn create_connector_by_req(
    client_pool: &Arc<ClientPool>,
    request: Request<MqttCreateConnectorRequest>,
) -> Result<(), MqttBrokerError> {
    let req = request.into_inner();
    let connector_type = parse_mqtt_connector_type(req.connector_type());
    connector_config_validator(&connector_type, &req.config)?;

    let config = broker_mqtt_conf();
    let storage = ConnectorStorage::new(client_pool.clone());
    let connector = MQTTConnector {
        cluster_name: config.cluster_name.clone(),
        connector_name: req.connector_name.clone(),
        connector_type: parse_mqtt_connector_type(req.connector_type()),
        config: req.config.clone(),
        topic_id: req.topic_id.clone(),
        status: MQTTStatus::Idle,
        broker_id: None,
        create_time: now_second(),
        update_time: now_second(),
    };

    storage
        .create_connector(connector)
        .await
        .map_err(|e| MqttBrokerError::CommonError(e.to_string()))?;

    Ok(())
}
// Update an existing connector
pub async fn update_connector_by_req(
    client_pool: &Arc<ClientPool>,
    request: Request<MqttUpdateConnectorRequest>,
) -> Result<(), MqttBrokerError> {
    let req = request.into_inner();
    let connector = serde_json::from_slice::<MQTTConnector>(&req.connector)
        .map_err(|e| MqttBrokerError::CommonError(e.to_string()))?;

    connector_config_validator(&connector.connector_type, &connector.config)?;

    let storage = ConnectorStorage::new(client_pool.clone());
    storage
        .update_connector(connector)
        .await
        .map_err(|e| MqttBrokerError::CommonError(e.to_string()))?;

    Ok(())
}

// Delete an existing connector
pub async fn delete_connector_by_req(
    client_pool: &Arc<ClientPool>,
    request: Request<MqttDeleteConnectorRequest>,
) -> Result<(), MqttBrokerError> {
    let req = request.into_inner();
    let config = broker_mqtt_conf();
    let storage = ConnectorStorage::new(client_pool.clone());

    storage
        .delete_connector(&config.cluster_name, &req.connector_name)
        .await
        .map_err(|e| MqttBrokerError::CommonError(e.to_string()))?;

    Ok(())
}

fn connector_config_validator(
    connector_type: &ConnectorType,
    config: &str,
) -> Result<(), MqttBrokerError> {
    match connector_type {
        ConnectorType::LocalFile => {
            let _file_config: LocalFileConnectorConfig = serde_json::from_str(config)?;
        }
        ConnectorType::Kafka => {
            let _kafka_config: KafkaConnectorConfig = serde_json::from_str(config)?;
        }
    }
    Ok(())
}

fn parse_mqtt_connector_type(connector_type: MqttConnectorType) -> ConnectorType {
    match connector_type {
        MqttConnectorType::File => ConnectorType::LocalFile,
        MqttConnectorType::Kafka => ConnectorType::Kafka,
    }
}
