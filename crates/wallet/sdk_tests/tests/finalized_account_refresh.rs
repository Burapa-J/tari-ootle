//   Copyright 2026 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

//! A finalized transaction updates the accounts it touches from its diff. When the diff shows an account holding a
//! vault the wallet does not know, the wallet refreshes that account from the network on the spot.

mod support;

use std::collections::BTreeSet;

use tari_engine_types::{
    component::{Component, ComponentBody, ComponentHeader},
    non_fungible::NonFungibleContainer,
    resource::Resource,
    resource_container::ResourceContainer,
    substate::{Substate, SubstateDiff, SubstateValue},
    vault::Vault,
};
use tari_ootle_common_types::{Epoch, SubstateVersion};
use tari_ootle_light_client::test_fixtures::ValidatorSetFixture;
use tari_ootle_transaction::{Transaction, TransactionId};
use tari_ootle_wallet_sdk::models::{KeyBranch, KeyId, NewAccountData, NonFungibleToken, WalletEvent};
use tari_ootle_wallet_sdk_services::{account_monitor::AccountScanner, notify::Notify};
use tari_template_builtin::ACCOUNT_TEMPLATE_ADDRESS;
use tari_template_lib::types::{
    Amount,
    ComponentAddress,
    Metadata,
    NonFungibleAddress,
    NonFungibleId,
    ResourceAddress,
    ResourceType,
    SubstateOwnerRule,
    VaultId,
    access_rules::{ComponentAccessRules, ResourceAccessRules},
};

use crate::support::{ProvingNetwork, Test, TestSdkSpec, TestWithNetwork, random_key};

const VERSION: SubstateVersion = SubstateVersion::new(3);

fn account_address() -> ComponentAddress {
    Test::test_account_address()
}

fn resource_address() -> ResourceAddress {
    "resource_7cbfe29101c24924b1b6ccefbfff98986d648622272ae24f7585dab5ffffffff"
        .parse()
        .unwrap()
}

fn vault_id(seed: u8) -> VaultId {
    VaultId::from_hex(&format!("{seed:02x}").repeat(32)).unwrap()
}

fn account_value(vaults: &[VaultId]) -> SubstateValue {
    Component {
        header: ComponentHeader {
            template_address: ACCOUNT_TEMPLATE_ADDRESS,
            owner_rule: SubstateOwnerRule::None,
            access_rules: ComponentAccessRules::new(),
            entity_id: Default::default(),
            upgrade_rule: Default::default(),
        },
        body: ComponentBody::from_cbor_value(tari_bor::to_value(&vaults.to_vec()).unwrap()),
    }
    .into()
}

fn vault_value(amount: u64) -> SubstateValue {
    Vault::new(ResourceContainer::public_fungible(resource_address(), amount)).into()
}

/// A wallet holding the test account with `held` as its only vault, and the network serving it.
fn wallet_holding(held: VaultId) -> (ProvingNetwork, TestWithNetwork<ProvingNetwork>) {
    let sets = (1..=2).map(ValidatorSetFixture::new).collect::<Vec<_>>();
    let network = ProvingNetwork::new(&sets, 0);
    let test = TestWithNetwork::without_accounts(network.clone());
    test.sdk()
        .resources_api()
        .upsert_resource(
            &resource_address(),
            &Resource::new(
                ResourceType::Fungible,
                SubstateOwnerRule::None,
                ResourceAccessRules::new(),
                Metadata::new(),
                None,
                None,
                6,
                false,
            ),
        )
        .unwrap();
    let accounts_api = test.sdk().accounts_api();
    accounts_api
        .add_account(
            Some("test"),
            &account_address(),
            KeyId::derived(KeyBranch::ViewOnlyKey, 0),
            KeyId::derived(KeyBranch::Account, 0),
            Epoch::zero(),
            true,
            true,
        )
        .unwrap();
    accounts_api
        .add_vault(
            account_address(),
            held,
            SubstateVersion::ZERO,
            resource_address(),
            ResourceType::Fungible,
            None,
            6,
        )
        .unwrap();
    (network, test)
}

fn scanner(test: &TestWithNetwork<ProvingNetwork>) -> AccountScanner<TestSdkSpec<ProvingNetwork>> {
    AccountScanner::new(Notify::new(1), test.sdk().clone())
}

fn diff_with_account(vaults: &[VaultId]) -> SubstateDiff {
    let mut diff = SubstateDiff::new();
    diff.up(account_address().into(), Substate::new(VERSION, account_value(vaults)));
    diff
}

#[tokio::test]
async fn a_finalized_account_holding_an_unknown_vault_is_refreshed() {
    let held = vault_id(1);
    let unknown = vault_id(2);
    let (network, test) = wallet_holding(held);
    network.serve_substate(account_address().into(), VERSION, account_value(&[held, unknown]), None);
    network.serve_substate(held.into(), VERSION, vault_value(100), None);
    network.serve_substate(unknown.into(), VERSION, vault_value(50), None);

    scanner(&test)
        .process_result(TransactionId::default(), &diff_with_account(&[held, unknown]), None)
        .await
        .unwrap();

    let vault = test.sdk().accounts_api().get_vault(&unknown).unwrap();
    assert_eq!(vault.account_address, account_address());
    assert_eq!(vault.revealed_balance, Amount::from(50u64));
}

#[tokio::test]
async fn a_finalized_account_holding_only_known_vaults_is_not_read_from_the_network() {
    let held = vault_id(1);
    // The network serves no account, so a refresh panics on reading it.
    let (_network, test) = wallet_holding(held);

    scanner(&test)
        .process_result(TransactionId::default(), &diff_with_account(&[held]), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn process_result_updates_existing_nft_mutable_data_and_filters_by_resource_address() {
    let nft_vault = vault_id(3);
    let (_network, test) = wallet_holding(vault_id(1));
    let nft_resource: ResourceAddress = "resource_1111111111111111111111111111111111111111111111111111111111111111"
        .parse()
        .unwrap();
    let other_nft_resource: ResourceAddress =
        "resource_2222222222222222222222222222222222222222222222222222222222222222"
            .parse()
            .unwrap();

    test.sdk()
        .resources_api()
        .upsert_resource(
            &nft_resource,
            &Resource::new(
                ResourceType::NonFungible,
                SubstateOwnerRule::None,
                ResourceAccessRules::new(),
                Metadata::new(),
                None,
                None,
                0,
                false,
            ),
        )
        .unwrap();
    test.sdk()
        .accounts_api()
        .add_vault(
            account_address(),
            nft_vault,
            SubstateVersion::ZERO,
            nft_resource,
            ResourceType::NonFungible,
            None,
            0,
        )
        .unwrap();

    let nft_id = NonFungibleId::from_u64(1);
    test.sdk()
        .non_fungible_api()
        .save_nft(&NonFungibleToken {
            vault_id: nft_vault,
            nft_id: nft_id.clone(),
            resource_address: nft_resource,
            data: tari_bor::Value::Text("immutable".to_string()),
            mutable_data: tari_bor::Value::Text("before".to_string()),
            is_burnt: false,
        })
        .unwrap();

    let mut diff = SubstateDiff::new();
    diff.up(account_address().into(), Substate::new(VERSION, account_value(&[nft_vault])));
    diff.up(
        nft_vault.into(),
        Substate::new(
            VERSION,
            Vault::new(ResourceContainer::non_fungible(
                nft_resource,
                BTreeSet::from([nft_id.clone()]),
            )),
        ),
    );
    // An unrelated resource's NFT with the same NonFungibleId appears first in the diff.
    diff.up(
        NonFungibleAddress::new(other_nft_resource, nft_id.clone()).into(),
        Substate::new(
            VERSION,
            NonFungibleContainer::new(
                tari_bor::Value::Text("wrong-resource".to_string()),
                tari_bor::Value::Text("wrong-mutable".to_string()),
            ),
        ),
    );
    diff.up(
        NonFungibleAddress::new(nft_resource, nft_id.clone()).into(),
        Substate::new(
            VERSION,
            NonFungibleContainer::new(
                tari_bor::Value::Text("immutable".to_string()),
                tari_bor::Value::Text("after".to_string()),
            ),
        ),
    );

    let notify = Notify::new(10);
    let mut rx = notify.subscribe();
    AccountScanner::new(notify, test.sdk().clone())
        .process_result(TransactionId::default(), &diff, None)
        .await
        .unwrap();

    let stored = test.sdk().non_fungible_api().get(nft_resource, nft_id).unwrap();
    assert_eq!(stored.data, tari_bor::Value::Text("immutable".to_string()));
    assert_eq!(stored.mutable_data, tari_bor::Value::Text("after".to_string()));
    assert!(matches!(
        rx.try_recv().expect("expected AccountChangedOnChain event"),
        WalletEvent::AccountChangedOnChain(event) if event.account_address == account_address()
    ));
}

#[tokio::test]
async fn process_result_recovers_new_account_from_stored_transaction_and_refresh_notifies_once() {
    let first_vault = vault_id(1);
    let second_vault = vault_id(2);
    let (network, test) = wallet_holding(first_vault);

    let tx = Transaction::builder_localnet(Epoch(100)).build_and_seal(&random_key());
    let tx_id = test
        .sdk()
        .transaction_api()
        .insert_new_transaction(
            tx,
            Some(NewAccountData {
                address: account_address(),
            }),
            &[],
            false,
            &[],
        )
        .unwrap();

    let notify = Notify::new(10);
    let mut rx = notify.subscribe();
    let account_scanner = AccountScanner::new(notify, test.sdk().clone());
    account_scanner
        .process_result(tx_id, &diff_with_account(&[first_vault]), None)
        .await
        .unwrap();

    assert!(matches!(
        rx.try_recv().expect("expected AccountCreatedOnChain event from stored transaction"),
        WalletEvent::AccountCreatedOnChain(event) if *event.account.component_address() == account_address()
    ));

    // Refreshing an account with two updated vaults must emit only one AccountChangedOnChain event.
    network.serve_substate(
        account_address().into(),
        VERSION,
        account_value(&[first_vault, second_vault]),
        None,
    );
    network.serve_substate(first_vault.into(), VERSION, vault_value(100), None);
    network.serve_substate(second_vault.into(), VERSION, vault_value(200), None);

    assert!(account_scanner.refresh_account(account_address()).await.unwrap());
    assert!(matches!(
        rx.try_recv().expect("expected single AccountChangedOnChain event"),
        WalletEvent::AccountChangedOnChain(event) if event.account_address == account_address()
    ));
    assert!(rx.try_recv().is_err(), "expected no duplicate AccountChangedOnChain event");
}
