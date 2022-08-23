//! This tool can be used to extract state updates from the local `pathfinder` database.
//! Extracted state updates will be stored in a directory called
//! `extracted_state_updates_<START_BLOCK_NUMBER>_<STOP_BLOCK_NUMBER>.
//!
//! `STOP_BLOCK_NUMBER` is optional, and otherwise `latest` is assumed.
//!
//! Each state update is saved in a separate file named `<BLOCK_NUMBER>.json`
//! and contains the state diff in the same format as
//!
//! `https://alpha-mainnet.starknet.io/feeder_gateway/get_state_update?blockNumber=<BLOCK_NUMBER>`
//!
//! or
//!
//! `https://alpha4.starknet.io/feeder_gateway/get_state_update?blockNumber=<BLOCK_NUMBER>`
//!
//! depending on network type.
fn print_usage_and_exit() -> ! {
    println!(
        "USAGE: {} db_file start_block_number [end_block_number]",
        std::env::args()
            .next()
            .as_deref()
            .unwrap_or("extract_state_updates")
    );
    std::process::exit(1)
}

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();

    let args_cnt = std::env::args().count();

    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| print_usage_and_exit());

    let start = std::env::args()
        .nth(2)
        .map(|start| {
            start
                .parse::<u64>()
                .unwrap_or_else(|_| print_usage_and_exit())
        })
        .unwrap_or_else(|| print_usage_and_exit());

    let stop = std::env::args().nth(3).map(|stop| {
        stop.parse::<u64>()
            .unwrap_or_else(|_| print_usage_and_exit())
    });

    let stop = match (stop, args_cnt) {
        (Some(stop), 4) if stop >= start => Some(stop),
        (None, 3) => None,
        _ => print_usage_and_exit(),
    };

    let path = std::path::PathBuf::from(path);
    let storage =
        pathfinder_lib::storage::Storage::migrate(path, pathfinder_lib::storage::JournalMode::WAL)
            .unwrap();

    let mut connection = storage.connection().unwrap();
    let transaction = connection.transaction().unwrap();

    let latest = pathfinder_lib::storage::StarknetBlocksTable::get_latest_number(&transaction)
        .unwrap()
        .unwrap()
        .get();

    drop(transaction);

    let stop = match stop {
        Some(stop) if stop <= latest => stop,
        Some(_) => print_usage_and_exit(),
        None => latest,
    };

    let work_dir = format!("./extracted_state_updates_{start}_{stop}");

    match std::fs::create_dir(&work_dir) {
        Ok(_) => {}
        Err(e) => tracing::warn!("{e}"),
    }

    let started = std::time::Instant::now();

    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<(pathfinder_lib::sequencer::reply::StateUpdate, u64)>(1);

    let keep_tx_alive = tx.clone();

    use pathfinder_lib::sequencer::{Client, ClientApi};

    let work_dir2 = work_dir.clone();

    let _handle_downloader = tokio::spawn(async move {
        let client = Client::new(pathfinder_lib::core::Chain::Mainnet).unwrap();

        while let Some((generated_state_update, block_number)) = rx.recv().await {
            tracing::info!("Downloading state update for block: {block_number}");

            let downloaded_state_update = client
                .state_update(BlockId::Number(StarknetBlockNumber::new_or_panic(
                    block_number,
                )))
                .await;

            let downloaded_state_update = downloaded_state_update.unwrap();

            let generated_storage_diffs = generated_state_update.state_diff.storage_diffs;
            let downloaded_storage_diffs = downloaded_state_update.state_diff.storage_diffs;

            let downloaded_but_not_generated = downloaded_storage_diffs
                .into_iter()
                .filter_map(
                    |(contract_address, downloaded_storage_diffs_for_contract)| {
                        match generated_storage_diffs.get(&contract_address) {
                            Some(generated_diffs) => {
                                let generated_storage_diffs_for_contract_map = generated_diffs
                                    .iter()
                                    .map(|x| (x.key, x.value))
                                    .collect::<HashMap<_, _>>();

                                let delta_for_contract = downloaded_storage_diffs_for_contract
                                    .into_iter()
                                    .filter_map(|x| {
                                        match generated_storage_diffs_for_contract_map.get(&x.key) {
                                            // Value is completely missing on the generated side
                                            None => {
                                                if x.value.0 == StarkHash::ZERO {
                                                    // Zero means there is __no value__, if it is the initial value
                                                    // Othewise it means that the leaf is deleted

                                                    // Let's indicate a deletion here
                                                    // TODO check: do we indicate a deletion in our state updates in the RPC? YES
                                                    // Some(x)

                                                    None
                                                } else {
                                                    // Ok this is some real value that is missing on the generated side
                                                    Some(x)
                                                }
                                            }
                                            // Value has not changed
                                            Some(generated_value)
                                                if x.value == *generated_value =>
                                            {
                                                None
                                            }
                                            // Value has changed
                                            Some(_) => Some(x),
                                        }
                                    })
                                    .collect::<Vec<_>>();

                                if delta_for_contract.is_empty() {
                                    None
                                } else {
                                    Some((contract_address, delta_for_contract))
                                }
                            }
                            // The entire contract is not in the generated diff
                            None => {
                                if downloaded_storage_diffs_for_contract.is_empty() {
                                    None
                                } else {
                                    let downloaded_storage_diffs_for_contract =
                                        downloaded_storage_diffs_for_contract
                                            .into_iter()
                                            .filter_map(|x| {
                                                if x.value.0 == StarkHash::ZERO {
                                                    None
                                                } else {
                                                    Some(x)
                                                }
                                            })
                                            .collect::<Vec<_>>();

                                    if downloaded_storage_diffs_for_contract.is_empty() {
                                        None
                                    } else {
                                        Some((
                                            contract_address,
                                            downloaded_storage_diffs_for_contract,
                                        ))
                                    }
                                }
                            }
                        }
                    },
                )
                .collect::<Vec<_>>();

            if !downloaded_but_not_generated.is_empty() {
                tracing::info!("++++ Extra downloaded for block: {block_number}");
                serde_json::to_writer(
                    &std::fs::File::create(format!(
                        "{work_dir}/{block_number}_extra_downloaded.json"
                    ))
                    .unwrap(),
                    &downloaded_but_not_generated,
                )
                .unwrap();
            }
        }
    });

    let _handle_dfs = tokio::task::spawn_blocking(move || {
        let transaction = connection.transaction().unwrap();

        extract_state_updates(&transaction, start, stop, |block_number, state_update| {
            serde_json::to_writer(
                &std::fs::File::create(format!("{work_dir2}/{block_number}.json")).unwrap(),
                &state_update,
            )
            .unwrap();
            tx.blocking_send((state_update, block_number)).unwrap();
        });
    });

    tracing::info!("Processing time: {:?}", started.elapsed());

    _handle_dfs.await.unwrap();
    // Drop tx only after all generated SUs have been sent
    drop(keep_tx_alive);
    _handle_downloader.await.unwrap();
}

use bitvec::{order::Msb0, slice::BitSlice};
use pathfinder_lib::{
    core::{
        BlockId, ContractAddress, ContractStateHash, GlobalRoot, StarknetBlockNumber,
        StorageAddress, StorageValue,
    },
    sequencer::reply::{
        state_update::{StateDiff, StorageDiff},
        StateUpdate,
    },
    state::{
        merkle_node::Node,
        merkle_tree::Visit,
        state_tree::{ContractsStateTree, GlobalStateTree},
    },
    storage::{ContractsStateTable, StarknetBlocksBlockId, StarknetBlocksTable},
};
use rusqlite::Transaction;
use stark_hash::StarkHash;
use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
};

pub fn extract_state_updates<F>(
    transaction: &Transaction<'_>,
    start_block: u64,
    end_block: u64,
    mut state_update_handler: F,
) where
    F: FnMut(u64, StateUpdate),
{
    // Contains **all** storage K-Vs up to but excluding the currently processed block, i.e.
    // the values contained describe the current state as of `(currently_processed_block - 1)`.
    let mut global_storage: HashMap<ContractAddress, HashMap<StorageAddress, StorageValue>> =
        HashMap::new();

    // If we don't start from genesis, the first iteration is used
    // to get the state diff from `0` up to `start_block - 1`, to
    // build the global storage cache.
    let (start_block, skip_first) = if start_block > 0 {
        (start_block - 1, true)
    } else {
        // Start from genesis
        (start_block, false)
    };

    // Extract storage diffs for each block, starting at genesis.
    // This is the naive way right now:
    // ```pseudocode
    //
    //     init empty global_KV_cache
    //
    //     foreach block in start_block..=end_block
    //         get all storage KVs (paths-leaves) for block
    //         if V not in global_KV_cache
    //             insert KV into block_state_diff
    //         yield block_state_diff
    //         merge block_state_diff into global_KV_cache
    // ```
    for block_number in start_block..=end_block {
        if skip_first && start_block > 0 && start_block == block_number {
            tracing::info!("Processing blocks 0-{block_number}/{end_block}");
        } else {
            tracing::info!("Processing block {block_number}/{end_block}");
        }

        // Contains all storage K-Vs for the current block that **differ** in any way from the `global_storage`
        // which is 1 block behind.
        let mut current_block_storage_delta: HashMap<
            ContractAddress,
            HashMap<StorageAddress, StorageValue>,
        > = HashMap::new();

        let block = StarknetBlocksTable::get(
            transaction,
            StarknetBlocksBlockId::Number(StarknetBlockNumber::new_or_panic(block_number)),
        )
        .unwrap()
        .unwrap();

        let global_tree = GlobalStateTree::load(transaction, block.root).unwrap();

        // The global tree visitor will find all contract state hashes that will point us
        // to each and every contract's state
        let mut global_visitor = |node: &Node, path: &BitSlice<Msb0, u8>| {
            if let Node::Leaf(contract_state_hash) = node {
                // Leaf value is the contract state hash for a particular contract address (which is the leaf path)
                // having a contract state hash we can get the contract state root
                // and traverse the entire contract state tree

                let contract_address =
                    ContractAddress::new_or_panic(StarkHash::from_bits(path).unwrap());
                let contract_state_root = ContractsStateTable::get_root(
                    &transaction,
                    ContractStateHash(*contract_state_hash),
                )
                .unwrap()
                .unwrap();
                let contract_state_tree =
                    ContractsStateTree::load(&transaction, contract_state_root).unwrap();

                // Any new changes to this contract's storage that occured withing this block go here
                let current_contract_delta = current_block_storage_delta
                    .entry(contract_address)
                    .or_default();

                // Storage for this contract as of `(this_block - 1)`
                let mut current_contract_global_storage = global_storage.entry(contract_address);

                let mut current_contract_all_keys = HashSet::new();

                // We use this visitor to inspect all the storage values for the current contract being processed in this very block
                // The visitor will find __new__ values or __changes__ in values but will not indicate __leaf removal__.
                let mut contract_visitor = |node: &Node, path: &BitSlice<Msb0, u8>| {
                    if let Node::Leaf(storage_value) = node {
                        // Leaf value is the storage value for a particular storage key (which is the leaf path)

                        let storage_key =
                            StorageAddress::new_or_panic(StarkHash::from_bits(path).unwrap());
                        let storage_value = StorageValue(*storage_value);

                        current_contract_all_keys.insert(storage_key);

                        match &mut current_contract_global_storage {
                            std::collections::hash_map::Entry::Occupied(
                                current_contract_all_diffs,
                            ) => {
                                let kvs_for_this_contract_all_diffs =
                                    current_contract_all_diffs.get_mut();

                                // Check for new values or changes in values
                                match kvs_for_this_contract_all_diffs.get(&storage_key) {
                                    // This K-V pair is completely new
                                    None => {
                                        if storage_value.0 == StarkHash::ZERO {
                                            tracing::info!("Remove leaf: {storage_key}");

                                            // The leaf was deleted
                                            kvs_for_this_contract_all_diffs
                                                .remove(&storage_key)
                                                .unwrap();
                                        } else {
                                            current_contract_delta
                                                .insert(storage_key, storage_value);
                                        }
                                    }
                                    // The leaf was deleted
                                    Some(_) if storage_value.0 == StarkHash::ZERO => {
                                        tracing::info!("Remove leaf: {storage_key}");

                                        kvs_for_this_contract_all_diffs
                                            .remove(&storage_key)
                                            .unwrap();
                                    }
                                    // Value has changed
                                    Some(old_value) if *old_value != storage_value => {
                                        current_contract_delta.insert(storage_key, storage_value);
                                    }
                                    // Value has not changed
                                    Some(_) => {
                                        // `current_contract_delta` contains an empty hash map
                                        // if there are no changes to the contract state in this entire block
                                        // in such case we will have to remove it from `current_block_storage_delta`
                                    }
                                }
                            }
                            std::collections::hash_map::Entry::Vacant(_) => {
                                // Ignore zero values which mean deletion/absent value
                                if storage_value.0 != StarkHash::ZERO {
                                    // Don't check in all_storage_diffs, this entire contract is a new entry globally
                                    current_contract_delta.insert(storage_key, storage_value);
                                } else {
                                    tracing::info!("Ignore leaf: {storage_key}");
                                }
                            }
                        }
                    }

                    ControlFlow::<(), _>::Continue(Visit::ContinueDeeper)
                };

                contract_state_tree.dfs(&mut contract_visitor);

                // Now check if any leaves are not present in the latest version of the tree and remove them
                // from the global cache
                match &mut current_contract_global_storage {
                    std::collections::hash_map::Entry::Occupied(
                        current_contract_global_storage,
                    ) => {
                        let current_contract_global_storage =
                            current_contract_global_storage.get_mut();
                        current_contract_global_storage
                            .retain(|k, _| current_contract_all_keys.contains(k));
                    }
                    std::collections::hash_map::Entry::Vacant(_) => {}
                }

                // Cleanup if it turned out that there were no updates to this contract in this block
                if current_contract_delta.is_empty() {
                    current_block_storage_delta.remove(&contract_address);
                }
            }

            ControlFlow::<(), _>::Continue(Visit::ContinueDeeper)
        };

        global_tree.dfs(&mut global_visitor);

        // FIXME old_root, deployed, declared
        let mut state_update = StateUpdate {
            block_hash: Some(block.hash),
            new_root: block.root,
            old_root: GlobalRoot(StarkHash::ZERO),
            state_diff: StateDiff {
                storage_diffs: HashMap::new(),
                deployed_contracts: vec![],
                declared_contracts: vec![],
            },
        };

        // Update global storage state with the processed block
        // We cannot just
        // `global_storage.extend(current_block_storage_delta.into_iter());`
        // as existing storage K-Vs for contracts would be wiped out instead of merged
        //
        // By the way: build a "sequencer style" storage diff for this block from current_block_storage_delta
        current_block_storage_delta
            .into_iter()
            .for_each(|(contract_address, storage_updates)| {
                let storage_diffs_for_this_contract = storage_updates
                    .iter()
                    .map(|(key, value)| StorageDiff {
                        key: *key,
                        value: *value,
                    })
                    .collect::<Vec<_>>();
                state_update
                    .state_diff
                    .storage_diffs
                    .insert(contract_address, storage_diffs_for_this_contract);

                global_storage
                    .entry(contract_address)
                    .or_default()
                    .extend(storage_updates.into_iter())
            });

        if !(skip_first && block_number == start_block) {
            state_update_handler(block_number, state_update);
        }
    }
}
