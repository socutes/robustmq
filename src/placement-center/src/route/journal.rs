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

use std::sync::Arc;

use common_base::tools::{now_mills, unique_id};
use grpc_clients::poll::ClientPool;
use prost::Message as _;
use protocol::placement_center::placement_center_journal::{
    CreateNextSegmentRequest, CreateShardRequest, DeleteSegmentRequest, DeleteShardRequest,
};

use crate::cache::journal::JournalCacheManager;
use crate::cache::placement::PlacementCacheManager;
use crate::controller::journal::call_node::{
    update_cache_by_add_segment, update_cache_by_add_shard, update_cache_by_delete_segment,
    update_cache_by_delete_shard,
};
use crate::core::error::PlacementCenterError;
use crate::core::journal::segmet::{create_first_segment, create_next_segment};
use crate::storage::engine;
use crate::storage::journal::segment::SegmentStorage;
use crate::storage::journal::shard::{ShardInfo, ShardStorage};
use crate::storage::rocksdb::RocksDBEngine;

#[derive(Clone)]
pub struct DataRouteJournal {
    rocksdb_engine_handler: Arc<RocksDBEngine>,
    engine_cache: Arc<JournalCacheManager>,
    cluster_cache: Arc<PlacementCacheManager>,
    client_poll: Arc<ClientPool>,
}

impl DataRouteJournal {
    pub fn new(
        rocksdb_engine_handler: Arc<RocksDBEngine>,
        engine_cache: Arc<JournalCacheManager>,
        cluster_cache: Arc<PlacementCacheManager>,
        client_poll: Arc<ClientPool>,
    ) -> Self {
        DataRouteJournal {
            rocksdb_engine_handler,
            engine_cache,
            cluster_cache,
            client_poll,
        }
    }
    pub fn create_shard(&self, value: Vec<u8>) -> Result<Vec<u8>, PlacementCenterError> {
        let req: CreateShardRequest = CreateShardRequest::decode(value.as_ref())?;

        let shard_info = ShardInfo {
            shard_uid: unique_id(),
            cluster_name: req.cluster_name.clone(),
            namespace: req.namespace.clone(),
            shard_name: req.shard_name.clone(),
            replica: req.replica,
            storage_mode: req.storage_model,
            start_segment_seq: 0,
            active_segment_seq: 0,
            last_segment_seq: 0,
            create_time: now_mills(),
        };

        // Save Shard && Update Cache
        let shard_storage = ShardStorage::new(self.rocksdb_engine_handler.clone());
        shard_storage.save(&shard_info)?;
        self.engine_cache.add_shard(&shard_info);

        // Create your first Segment
        let segment = create_first_segment(
            &shard_info,
            &self.engine_cache,
            &self.cluster_cache,
            &self.rocksdb_engine_handler,
        )?;

        // Update storage engine node cache
        update_cache_by_add_shard(
            req.cluster_name.clone(),
            self.cluster_cache.clone(),
            self.client_poll.clone(),
            shard_info,
        );

        update_cache_by_add_segment(
            req.cluster_name.clone(),
            self.cluster_cache.clone(),
            self.client_poll.clone(),
            segment.clone(),
        );

        Ok(serde_json::to_vec(&segment)?)
    }

    pub fn delete_shard(&self, value: Vec<u8>) -> Result<(), PlacementCenterError> {
        let req = DeleteShardRequest::decode(value.as_ref())?;
        let cluster_name = req.cluster_name;
        let namespace = req.namespace;
        let shard_name = req.shard_name;

        let res = self
            .engine_cache
            .get_shard(&cluster_name, &namespace, &shard_name);
        if res.is_none() {
            return Err(PlacementCenterError::ShardDoesNotExist(shard_name));
        }

        let shard = res.unwrap();

        // Delete all segment
        let segment_storage = SegmentStorage::new(self.rocksdb_engine_handler.clone());
        let segment_list = segment_storage.list_by_shard(&cluster_name, &namespace, &shard_name)?;
        for segment in segment_list {
            segment_storage.delete(&cluster_name, &namespace, &shard_name, segment.segment_seq)?;
        }

        // Delete shard info
        let shard_storage = ShardStorage::new(self.rocksdb_engine_handler.clone());
        shard_storage.delete(&cluster_name, &namespace, &shard_name)?;

        self.engine_cache
            .remove_shard(&cluster_name, &namespace, &shard_name);

        // Update storage engine node cache
        update_cache_by_delete_shard(
            cluster_name,
            self.cluster_cache.clone(),
            self.client_poll.clone(),
            shard,
        );

        Ok(())
    }

    pub fn create_next_segment(&self, value: Vec<u8>) -> Result<Vec<u8>, PlacementCenterError> {
        let req = CreateNextSegmentRequest::decode(value.as_ref())?;

        let cluster_name = req.cluster_name;
        let shard_name = req.shard_name;
        let namespace = req.namespace;

        let res = self
            .engine_cache
            .get_shard(&cluster_name, &namespace, &shard_name);
        if res.is_none() {
            return Err(PlacementCenterError::ShardDoesNotExist(shard_name));
        }

        let shard = res.unwrap();

        if (shard.last_segment_seq - shard.active_segment_seq) >= req.active_segment_next_num {
            return Err(PlacementCenterError::ShardHasEnoughSegment(shard_name));
        }

        let segment = create_next_segment(
            &shard,
            &self.engine_cache,
            &self.cluster_cache,
            &self.rocksdb_engine_handler,
        )?;

        update_cache_by_add_segment(
            cluster_name,
            self.cluster_cache.clone(),
            self.client_poll.clone(),
            segment.clone(),
        );

        Ok(serde_json::to_vec(&segment)?)
    }

    pub fn delete_segment(&self, value: Vec<u8>) -> Result<(), PlacementCenterError> {
        let req: DeleteSegmentRequest = DeleteSegmentRequest::decode(value.as_ref())?;
        let cluster_name = req.cluster_name;
        let namespace = req.namespace;
        let shard_name = req.shard_name;
        let segment_seq = req.segment_seq;

        let shard_res = self
            .engine_cache
            .get_shard(&cluster_name, &namespace, &shard_name);
        if shard_res.is_none() {
            return Err(PlacementCenterError::ShardDoesNotExist(shard_name));
        }

        let shard = shard_res.unwrap();

        if segment_seq > shard.start_segment_seq {
            return Err(PlacementCenterError::InvalidSegmentGreaterThan(
                segment_seq,
                shard.start_segment_seq,
            ));
        }
        if segment_seq < shard.start_segment_seq {
            return Err(PlacementCenterError::InvalidSegmentLessThan(
                segment_seq,
                shard.start_segment_seq,
            ));
        }

        let segment_res =
            self.engine_cache
                .get_segment(&cluster_name, &namespace, &shard_name, segment_seq);

        if segment_res.is_none() {
            return Err(PlacementCenterError::SegmentDoesNotExist(shard_name));
        }

        let segment = segment_res.unwrap();

        let segment_storage = SegmentStorage::new(self.rocksdb_engine_handler.clone());
        segment_storage.delete(&cluster_name, &namespace, &shard_name, segment_seq)?;

        self.engine_cache
            .remove_segment(&cluster_name, &namespace, &shard_name, segment_seq);

        let shard_storage = ShardStorage::new(self.rocksdb_engine_handler.clone());
        shard_storage.save(&shard)?;

        update_cache_by_delete_segment(
            cluster_name,
            self.cluster_cache.clone(),
            self.client_poll.clone(),
            segment.clone(),
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_base::config::placement_center::{
        placement_center_test_conf, PlacementCenterConfig,
    };
    use common_base::tools::{now_mills, unique_id};
    use grpc_clients::poll::ClientPool;
    use metadata_struct::journal::node_extend::JournalNodeExtend;
    use metadata_struct::placement::node::BrokerNode;
    use prost::Message;
    use protocol::journal_server::journal_engine::ShardStorageModel;
    use protocol::placement_center::placement_center_inner::ClusterType;
    use protocol::placement_center::placement_center_journal::{
        CreateNextSegmentRequest, CreateShardRequest, DeleteSegmentRequest, DeleteShardRequest,
    };
    use rocksdb_engine::RocksDBEngine;

    use super::DataRouteJournal;
    use crate::cache::journal::JournalCacheManager;
    use crate::cache::placement::PlacementCacheManager;
    use crate::storage::journal::segment::{SegmentInfo, SegmentStorage};
    use crate::storage::journal::shard::{ShardInfo, ShardStorage};
    use crate::storage::rocksdb::{column_family_list, storage_data_fold};

    #[tokio::test]
    async fn shard_test() {
        let (client_poll, config, engine_cache, cluster_cache, rocksdb_engine_handler) =
            build_ins();
        let route = DataRouteJournal::new(
            rocksdb_engine_handler.clone(),
            engine_cache.clone(),
            cluster_cache,
            client_poll,
        );

        let cluster_name = config.cluster_name.clone();
        let namespace = "n1".to_string();
        let shard_name = "s1".to_string();

        // create shard
        let request = CreateShardRequest {
            cluster_name: cluster_name.clone(),
            namespace: namespace.clone(),
            shard_name: shard_name.clone(),
            replica: 2,
            storage_model: ShardStorageModel::Sequential.as_str_name().to_string(),
        };

        let value = CreateShardRequest::encode_to_vec(&request);
        let data = route.create_shard(value).unwrap();
        let segment = serde_json::from_slice::<SegmentInfo>(&data).unwrap();
        assert_eq!(segment.replicas.len(), 2);
        assert_eq!(segment.segment_seq, 0);

        assert!(engine_cache
            .get_shard(&cluster_name, &namespace, &shard_name)
            .is_some());

        assert!(engine_cache
            .get_segment(&cluster_name, &namespace, &shard_name, 0)
            .is_some());

        let shard_storage = ShardStorage::new(rocksdb_engine_handler.clone());
        assert!(shard_storage
            .get(&cluster_name, &namespace, &shard_name)
            .unwrap()
            .is_some());

        let segment_storage = SegmentStorage::new(rocksdb_engine_handler.clone());
        assert!(segment_storage
            .get(&cluster_name, &namespace, &shard_name, 0)
            .unwrap()
            .is_some());

        let shard = engine_cache
            .get_shard(&cluster_name, &namespace, &shard_name)
            .unwrap();
        assert_eq!(shard.last_segment_seq, 0);

        // create next segment
        let request = CreateNextSegmentRequest {
            cluster_name: cluster_name.clone(),
            namespace: namespace.clone(),
            shard_name: shard_name.clone(),
            active_segment_next_num: 1,
        };
        let value = CreateNextSegmentRequest::encode_to_vec(&request);
        let segment_res = route.create_next_segment(value.clone()).unwrap();
        let segment = serde_json::from_slice::<SegmentInfo>(&segment_res).unwrap();
        assert_eq!(segment.segment_seq, 1);
        assert_eq!(segment.replicas.len(), 2);

        let shard = engine_cache
            .get_shard(&cluster_name, &namespace, &shard_name)
            .unwrap();
        assert_eq!(shard.last_segment_seq, 1);

        assert!(engine_cache
            .get_segment(&cluster_name, &namespace, &shard_name, 1)
            .is_some());

        assert!(segment_storage
            .get(&cluster_name, &namespace, &shard_name, 1)
            .unwrap()
            .is_some());

        // create next next segment
        let res = route.create_next_segment(value);
        assert!(res.is_err());

        // delete min next segment
        let request = DeleteSegmentRequest {
            cluster_name: cluster_name.clone(),
            namespace: namespace.clone(),
            shard_name: shard_name.clone(),
            segment_seq: 1,
        };

        let value = DeleteSegmentRequest::encode_to_vec(&request);
        assert!(route.delete_segment(value).is_err());

        // delete min segment
        let request = DeleteSegmentRequest {
            cluster_name: cluster_name.clone(),
            namespace: namespace.clone(),
            shard_name: shard_name.clone(),
            segment_seq: 0,
        };

        let value = DeleteSegmentRequest::encode_to_vec(&request);
        assert!(route.delete_segment(value).is_ok());

        assert!(engine_cache
            .get_segment(&cluster_name, &namespace, &shard_name, 0)
            .is_none());

        assert!(segment_storage
            .get(&cluster_name, &namespace, &shard_name, 0)
            .unwrap()
            .is_none());

        let shard = engine_cache
            .get_shard(&cluster_name, &namespace, &shard_name)
            .unwrap();
        assert_eq!(shard.start_segment_seq, 1);

        // delete shard
        let request = DeleteShardRequest {
            cluster_name: cluster_name.clone(),
            namespace: namespace.clone(),
            shard_name: shard_name.clone(),
        };
        let value = DeleteShardRequest::encode_to_vec(&request);
        assert!(route.delete_shard(value).is_ok());

        assert!(engine_cache
            .get_shard(&cluster_name, &namespace, &shard_name)
            .is_none());
        assert!(shard_storage
            .get(&cluster_name, &namespace, &shard_name)
            .unwrap()
            .is_none());
    }

    fn build_ins() -> (
        Arc<ClientPool>,
        PlacementCenterConfig,
        Arc<JournalCacheManager>,
        Arc<PlacementCacheManager>,
        Arc<RocksDBEngine>,
    ) {
        let client_poll = Arc::new(ClientPool::new(3));
        let config = placement_center_test_conf();
        let rocksdb_engine_handler = Arc::new(RocksDBEngine::new(
            &storage_data_fold(&config.rocksdb.data_path),
            config.rocksdb.max_open_files.unwrap(),
            column_family_list(),
        ));
        let engine_cache = Arc::new(JournalCacheManager::new());
        let cluster_cache = Arc::new(PlacementCacheManager::new(rocksdb_engine_handler.clone()));

        let shard_info = ShardInfo {
            shard_uid: unique_id(),
            cluster_name: config.cluster_name.clone(),
            namespace: "n1".to_string(),
            shard_name: "s1".to_string(),
            replica: 2,
            start_segment_seq: 0,
            active_segment_seq: 0,
            last_segment_seq: 0,
            storage_mode: "m1".to_string(),
            create_time: now_mills(),
        };

        engine_cache.add_shard(&shard_info);

        add_node(config.clone(), &cluster_cache);
        (
            client_poll,
            config,
            engine_cache,
            cluster_cache,
            rocksdb_engine_handler,
        )
    }

    fn add_node(config: PlacementCenterConfig, cluster_cache: &Arc<PlacementCacheManager>) {
        let extend_info = JournalNodeExtend {
            data_fold: vec!["/tmp/t1".to_string(), "/tmp/t2".to_string()],
        };

        let node = BrokerNode {
            cluster_name: config.cluster_name.clone(),
            cluster_type: ClusterType::JournalServer.as_str_name().to_string(),
            create_time: now_mills(),
            extend: serde_json::to_string(&extend_info).unwrap(),
            node_id: 1,
            node_inner_addr: "".to_string(),
            node_ip: "".to_string(),
        };
        cluster_cache.add_broker_node(node);

        let node = BrokerNode {
            cluster_name: config.cluster_name.clone(),
            cluster_type: ClusterType::JournalServer.as_str_name().to_string(),
            create_time: now_mills(),
            extend: serde_json::to_string(&extend_info).unwrap(),
            node_id: 2,
            node_inner_addr: "".to_string(),
            node_ip: "".to_string(),
        };
        cluster_cache.add_broker_node(node);

        let node = BrokerNode {
            cluster_name: config.cluster_name.clone(),
            cluster_type: ClusterType::JournalServer.as_str_name().to_string(),
            create_time: now_mills(),
            extend: serde_json::to_string(&extend_info).unwrap(),
            node_id: 3,
            node_inner_addr: "".to_string(),
            node_ip: "".to_string(),
        };
        cluster_cache.add_broker_node(node);
    }
}
