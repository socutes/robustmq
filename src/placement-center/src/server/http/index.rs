/*
 * Copyright (c) 2023 RobustMQ Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use super::server::HttpServerState;
use axum::extract::State;
use common_base::{http_response::success_response, metrics::dump_metrics};
use metadata_struct::placement::broker_node::BrokerNode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct IndexResponse {
    pub local: BrokerNode,
    pub node_lists: HashMap<u64, BrokerNode>,
    pub raft: RaftInfo,
}

#[derive(Serialize, Deserialize)]
pub struct RaftInfo {
    pub role: String,
    pub first_index: u64,
    pub last_index: u64,
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub voters_outgoing: Vec<u64>,
    pub learners_next: Vec<u64>,
    pub auto_leave: bool,
    pub uncommit_index: HashMap<u64, i8>,
}

pub async fn index(State(state): State<HttpServerState>) -> String {
    let storage = state.raft_storage.read().unwrap();
    let placement_cache = state.placement_cache.read().unwrap();
    let hs = storage.hard_state();
    let cs = storage.conf_state();
    let uncommit_index = storage.uncommit_index();

    let raft_info = RaftInfo {
        role: format!("{:?}", placement_cache.raft_role),
        first_index: storage.first_index(),
        last_index: storage.last_index(),
        term: hs.term,
        vote: hs.vote,
        commit: hs.commit,
        voters: cs.voters.to_vec(),
        learners: cs.learners.to_vec(),
        voters_outgoing: cs.voters_outgoing.to_vec(),
        learners_next: cs.learners_next.to_vec(),
        auto_leave: cs.auto_leave,
        uncommit_index: uncommit_index,
    };

    let resp = IndexResponse {
        local: placement_cache.local.clone(),
        node_lists: placement_cache.peers.clone(),
        raft: raft_info,
    };

    return success_response(resp);
}

pub async fn metrics() -> String {
    return dump_metrics();
}
