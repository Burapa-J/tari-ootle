//   Copyright 2026 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

mod support;

use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    sync::{Arc, Mutex},
};

use futures::StreamExt;
use ootle_byte_type::ToByteType;
use tari_engine_types::{Epoch, substate::SubstateId};
use tari_indexer_client::types::{EpochCheckpointEntry, ValidatorSetCommittee, WatchedSubstateItem};
use tari_ootle_common_types::{StateVersion, SubstateVersion, shard::Shard};
use tari_ootle_transaction::{Transaction, TransactionEnvelope, TransactionId};
use tari_ootle_wallet_sdk::{
    models::{
        EndOfShard,
        KeyBranch,
        KeyId,
        OutputStatus,
        StartOfShard,
        StealthOutputModel,
        UtxoBurnt,
        UtxoSpent,
        UtxoUnspent,
        UtxoUpdatePayload,
        WalletEvent,
        WalletUtxoUpdate,
    },
    network::{
        FetchedSubstates,
        FetchedUtxos,
        PlacedTagAndNoncePair,
        SubstateQueryResult,
        TransactionFinalizedStream,
        TransactionQueryResult,
        UtxoUpdateStream,
        WalletNetworkInterface,
    },
    storage::{ReadableWalletStore, WalletStoreReader},
};
use tari_ootle_wallet_sdk_services::{notify::Notify, utxo_scanner::UtxoScanner};
use tari_template_abi::TemplateDef;
use tari_template_lib::types::{
    EncryptedData,
    ResourceAddress,
    TemplateAddress,
    UtxoId,
    constants::STEALTH_TARI_RESOURCE_ADDRESS,
    crypto::{PedersenCommitmentBytes, RistrettoPublicKeyBytes, UtxoTag},
    stealth::SpendAuthorization,
};

use crate::support::{Test, TestWithNetwork, random_keypair};

/// A network that answers each UTXO update request with the next scripted stream.
#[derive(Debug, Clone, Default)]
struct ScriptedUtxoNetwork {
    streams: Arc<Mutex<VecDeque<Vec<UtxoUpdatePayload>>>>,
}

impl ScriptedUtxoNetwork {
    fn push_stream(&self, payloads: Vec<UtxoUpdatePayload>) {
        self.streams.lock().unwrap().push_back(payloads);
    }
}

impl WalletNetworkInterface for ScriptedUtxoNetwork {
    type Error = Infallible;

    async fn query_substate(
        &self,
        _: &SubstateId,
        _: Option<SubstateVersion>,
        _: bool,
        _: bool,
    ) -> Result<SubstateQueryResult, Self::Error> {
        panic!("ScriptedUtxoNetwork::query_substate called")
    }

    async fn get_substates(&self, _: &[SubstateId], _: bool) -> Result<FetchedSubstates, Self::Error> {
        panic!("ScriptedUtxoNetwork::get_substates called")
    }

    async fn submit_transaction(&self, _: Transaction) -> Result<TransactionId, Self::Error> {
        panic!("ScriptedUtxoNetwork::submit_transaction called")
    }

    async fn submit_transaction_envelope(&self, _: TransactionEnvelope) -> Result<TransactionId, Self::Error> {
        panic!("ScriptedUtxoNetwork::submit_transaction_envelope called")
    }

    async fn submit_dry_run_transaction(&self, _: Transaction) -> Result<TransactionQueryResult, Self::Error> {
        panic!("ScriptedUtxoNetwork::submit_dry_run_transaction called")
    }

    async fn query_transaction_result(&self, _: TransactionId, _: bool) -> Result<TransactionQueryResult, Self::Error> {
        panic!("ScriptedUtxoNetwork::query_transaction_result called")
    }

    async fn subscribe_transaction_finalized(&self) -> Result<TransactionFinalizedStream<Self::Error>, Self::Error> {
        Ok(futures::stream::pending().boxed())
    }

    async fn fetch_template_definition(&self, _: TemplateAddress) -> Result<TemplateDef, Self::Error> {
        panic!("ScriptedUtxoNetwork::fetch_template_definition called")
    }

    async fn stream_stealth_utxo_updates(
        &self,
        _from_epoch: Epoch,
        _resource_address: ResourceAddress,
        _shard_state_versions: Vec<(Shard, StateVersion)>,
        _unspent_only: bool,
    ) -> Result<UtxoUpdateStream<Self::Error>, Self::Error> {
        let payloads = self.streams.lock().unwrap().pop_front().unwrap_or_default();
        Ok(futures::stream::iter(payloads.into_iter().map(Ok)).boxed())
    }

    async fn list_watched_substates(
        &self,
        _: Option<TemplateAddress>,
        _: Option<u64>,
        _: Option<u64>,
    ) -> Result<Vec<WatchedSubstateItem>, Self::Error> {
        panic!("ScriptedUtxoNetwork::list_watched_substates called")
    }

    async fn fetch_utxos(
        &self,
        _: ResourceAddress,
        _: Vec<PlacedTagAndNoncePair>,
    ) -> Result<FetchedUtxos, Self::Error> {
        panic!("ScriptedUtxoNetwork::fetch_utxos called")
    }

    async fn get_latest_epoch_checkpoint(&self, _: Shard) -> Result<EpochCheckpointEntry, Self::Error> {
        panic!("ScriptedUtxoNetwork::get_latest_epoch_checkpoint called")
    }

    async fn get_latest_shard_group_checkpoints(&self) -> Result<Vec<EpochCheckpointEntry>, Self::Error> {
        panic!("ScriptedUtxoNetwork::get_latest_shard_group_checkpoints called")
    }

    async fn list_epoch_checkpoints(
        &self,
        _: Epoch,
        _: u32,
        _: Shard,
    ) -> Result<Vec<EpochCheckpointEntry>, Self::Error> {
        panic!("ScriptedUtxoNetwork::list_epoch_checkpoints called")
    }

    async fn get_committee(&self, _: Epoch, _: Shard) -> Result<ValidatorSetCommittee, Self::Error> {
        panic!("ScriptedUtxoNetwork::get_committee called")
    }

    async fn get_current_epoch(&self) -> Result<Epoch, Self::Error> {
        panic!("ScriptedUtxoNetwork::get_current_epoch called")
    }

    async fn get_consensus_epoch(&self) -> Result<Option<Epoch>, Self::Error> {
        panic!("ScriptedUtxoNetwork::get_consensus_epoch called")
    }

    async fn wait_until_ready(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

const SHARD: Shard = Shard::from_u32(0);

/// Builds a UTXO that the test account's view key recognises as its own.
fn owned_utxo(test: &TestWithNetwork<ScriptedUtxoNetwork>) -> UtxoUnspent {
    let sdk = test.sdk();
    let view_key = sdk
        .key_manager_api()
        .get_key(KeyId::derived(KeyBranch::ViewOnlyKey, 0))
        .unwrap();
    let (_, public_nonce) = random_keypair();
    let tag = sdk.stealth_crypto_api().derive_stealth_output_tag(
        sdk.network(),
        &view_key.secret,
        &public_nonce,
        &STEALTH_TARI_RESOURCE_ADDRESS,
    );
    UtxoUnspent {
        tag,
        public_nonce: public_nonce.to_byte_type(),
    }
}

fn start_of_shard(shard: Shard, max_state_version: u64, utxo: UtxoUnspent) -> UtxoUpdatePayload {
    UtxoUpdatePayload {
        sos: Some(StartOfShard {
            shard,
            max_state_version: StateVersion::from(max_state_version),
            has_more: false,
            spends_incomplete: false,
        }),
        update: Some(WalletUtxoUpdate::Unspent(utxo)),
        eos: None,
    }
}

fn end_of_shard(max_state_version: u64, utxo: UtxoUnspent) -> UtxoUpdatePayload {
    UtxoUpdatePayload {
        sos: None,
        update: Some(WalletUtxoUpdate::Unspent(utxo)),
        eos: Some(EndOfShard {
            max_state_version: StateVersion::from(max_state_version),
        }),
    }
}

async fn scan(test: &TestWithNetwork<ScriptedUtxoNetwork>) {
    let sdk = test.sdk();
    let account = sdk
        .accounts_api()
        .get_account_by_address(&Test::test_account_address())
        .unwrap();
    UtxoScanner::new(sdk.clone(), Notify::new(100))
        .scan_and_enqueue_utxos(&account, &STEALTH_TARI_RESOURCE_ADDRESS)
        .await
        .unwrap();
}

fn saved_cursor(test: &TestWithNetwork<ScriptedUtxoNetwork>, shard: Shard) -> Option<StateVersion> {
    test.sdk()
        .store()
        .with_read_tx(|tx| tx.shard_state_version_get(&Test::test_account_address(), &STEALTH_TARI_RESOURCE_ADDRESS))
        .unwrap()
        .get(&shard)
        .copied()
        .filter(|v| *v != StateVersion::zero())
}

fn num_queued(test: &TestWithNetwork<ScriptedUtxoNetwork>) -> usize {
    test.sdk()
        .store()
        .with_read_tx(|tx| tx.utxo_process_queue_fetch_batch(None, 100))
        .unwrap()
        .entries
        .values()
        .map(HashMap::len)
        .sum()
}

#[tokio::test]
async fn a_shard_without_its_end_marker_keeps_its_cursor() {
    let network = ScriptedUtxoNetwork::default();
    let test = TestWithNetwork::with_network(network.clone());
    let first = owned_utxo(&test);
    let second = owned_utxo(&test);

    // The stream ends before the shard's last update and its end marker arrive.
    network.push_stream(vec![start_of_shard(SHARD, 2, first.clone())]);
    scan(&test).await;
    assert_eq!(num_queued(&test), 1);
    assert_eq!(saved_cursor(&test, SHARD), None);

    network.push_stream(vec![start_of_shard(SHARD, 2, first), end_of_shard(5, second)]);
    scan(&test).await;
    assert_eq!(num_queued(&test), 2);
    assert_eq!(saved_cursor(&test, SHARD), Some(StateVersion::from(2)));
}

#[tokio::test]
async fn a_shard_followed_by_another_before_its_end_marker_keeps_its_cursor() {
    let network = ScriptedUtxoNetwork::default();
    let test = TestWithNetwork::with_network(network.clone());
    let other_shard = Shard::from_u32(1);

    network.push_stream(vec![
        start_of_shard(SHARD, 2, owned_utxo(&test)),
        start_of_shard(other_shard, 3, owned_utxo(&test)),
        end_of_shard(7, owned_utxo(&test)),
    ]);
    scan(&test).await;
    assert_eq!(num_queued(&test), 3);
    assert_eq!(saved_cursor(&test, SHARD), None);
    assert_eq!(saved_cursor(&test, other_shard), Some(StateVersion::from(3)));
}

fn insert_stealth_output(
    test: &TestWithNetwork<ScriptedUtxoNetwork>,
    owner_account: tari_template_lib::types::ComponentAddress,
    commitment_seed: u8,
) -> UtxoId {
    let commitment = PedersenCommitmentBytes::from([commitment_seed; 32]);
    test.sdk()
        .stealth_outputs_api()
        .add_output(&StealthOutputModel {
            owner_account,
            resource_address: STEALTH_TARI_RESOURCE_ADDRESS,
            commitment,
            value: 100,
            sender_public_nonce: RistrettoPublicKeyBytes::default(),
            view_only_key_id: KeyId::derived(KeyBranch::ViewOnlyKey, 0),
            owner_key_id: Some(KeyId::derived(KeyBranch::Account, 0)),
            encrypted_data: EncryptedData::try_from(vec![0; EncryptedData::min_size()]).unwrap(),
            tag_byte: UtxoTag::new(0),
            memo: None,
            auth: SpendAuthorization::Key(RistrettoPublicKeyBytes::default()),
            minimum_value_promise: 0,
            status: OutputStatus::Unspent,
            is_burnt: false,
            is_frozen: false,
            is_on_chain: true,
            is_condition_spendable: true,
            lock_id: None,
            proven_up: None,
            spent_version: None,
        })
        .unwrap();
    UtxoId::from(commitment)
}

#[tokio::test]
async fn spent_and_burnt_utxo_events_name_the_utxo_owner_account_not_the_scanning_account() {
    let network = ScriptedUtxoNetwork::default();
    let test = TestWithNetwork::with_network(network.clone());
    let sdk = test.sdk();

    let second_addr = sdk.key_manager_api().derive_account_address(1).unwrap();
    let second_account = sdk
        .accounts_api()
        .create_account(Some("second"), false, second_addr)
        .unwrap();
    let second_component = *second_account.account.component_address();

    // Ensure the scanning account has at least one live output so `unspent_only` is false.
    let _live_on_first = insert_stealth_output(&test, Test::test_account_address(), 1);
    let spent_id = insert_stealth_output(&test, second_component, 2);
    let burnt_id = insert_stealth_output(&test, second_component, 3);

    network.push_stream(vec![
        UtxoUpdatePayload {
            sos: Some(StartOfShard {
                shard: SHARD,
                max_state_version: StateVersion::from(2),
                has_more: false,
                spends_incomplete: false,
            }),
            update: Some(WalletUtxoUpdate::Spent(UtxoSpent {
                id: spent_id,
                version: SubstateVersion::new(1),
            })),
            eos: None,
        },
        UtxoUpdatePayload {
            sos: None,
            update: Some(WalletUtxoUpdate::Burnt(UtxoBurnt {
                id: burnt_id,
                version: SubstateVersion::new(1),
            })),
            eos: Some(EndOfShard {
                max_state_version: StateVersion::from(3),
            }),
        },
    ]);

    let notify = Notify::new(100);
    let mut rx = notify.subscribe();
    let first_account = sdk
        .accounts_api()
        .get_account_by_address(&Test::test_account_address())
        .unwrap();
    UtxoScanner::new(sdk.clone(), notify)
        .scan_and_enqueue_utxos(&first_account, &STEALTH_TARI_RESOURCE_ADDRESS)
        .await
        .unwrap();

    let first_event = rx.try_recv().expect("expected UtxoSpent event");
    let second_event = rx.try_recv().expect("expected UtxoBurnt event");
    match first_event {
        WalletEvent::UtxoSpent(event) => {
            assert_eq!(event.account_address, second_component);
            assert_eq!(*event.address.id(), spent_id);
        },
        other => panic!("unexpected first event: {other:?}"),
    }
    match second_event {
        WalletEvent::UtxoBurnt(event) => {
            assert_eq!(event.account_address, second_component);
            assert_eq!(*event.address.id(), burnt_id);
        },
        other => panic!("unexpected second event: {other:?}"),
    }
}
