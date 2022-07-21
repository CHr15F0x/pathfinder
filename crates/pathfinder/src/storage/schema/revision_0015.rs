use std::collections::HashMap;

use anyhow::Context;
use bitvec::{order::Msb0, slice::BitSlice};
use rusqlite::Transaction;
use stark_hash::StarkHash;

use crate::core::{
    ContractAddress, ContractStateHash, GlobalRoot, StarknetBlockNumber, StorageAddress,
    StorageValue,
};
use crate::sequencer::reply::state_update::{StateDiff, StorageDiff};
use crate::sequencer::reply::StateUpdate;
use crate::state::{
    merkle_node::Node,
    state_tree::{ContractsStateTree, GlobalStateTree},
};
use crate::storage::{ContractsStateTable, StarknetBlocksBlockId, StarknetBlocksTable};

/// Adds `starknet_state_updates` table.
pub(crate) fn migrate(transaction: &Transaction<'_>) -> anyhow::Result<()> {
    transaction
        .execute_batch(
            r"
            CREATE TABLE starknet_state_updates (
                block_hash BLOB PRIMARY KEY NOT NULL,
                data BLOB NOT NULL,
                FOREIGN KEY(block_hash) REFERENCES starknet_blocks(hash)
                ON DELETE CASCADE
            );",
        )
        .context("Creating starknet_state_updates table")?;

    Ok(())
}

pub fn test_state_update_extraction(
    transaction: &Transaction<'_>,
    num_blocks: u64,
) -> Vec<StateUpdate> {
    let mut state_updates = vec![];

    // Contains **all** storage K-Vs up to but excluding the currently processed block, i.e.
    // the values contained describe the current state as of `(currently_processed_block - 1)`.
    let mut global_storage: HashMap<ContractAddress, HashMap<StorageAddress, StorageValue>> =
        HashMap::new();

    // Extract storage diffs for each block, starting at genesis.
    // This is the naive way right now:
    // ```pseudocode
    //
    //     init empty global_KV_cache
    //
    //     foreach block in 0..=latest
    //         get all storage KVs (paths-leaves) for block
    //         if V not in global_KV_cache
    //             insert KV into block_state_diff
    //         yield block_state_diff
    //         merge block_state_diff into global_KV_cache
    // ```
    for block_number in 0..num_blocks {
        tracing::info!("Processing block {block_number}/{num_blocks}");

        // Contains all storage K-Vs for the current block that **differ** in any way from the `global_storage`
        // which is 1 block behind.
        let mut current_block_storage_delta: HashMap<
            ContractAddress,
            HashMap<StorageAddress, StorageValue>,
        > = HashMap::new();

        let block = StarknetBlocksTable::get(
            transaction,
            StarknetBlocksBlockId::Number(StarknetBlockNumber(block_number)),
        )
        .unwrap()
        .unwrap();

        let global_tree = GlobalStateTree::load(transaction, block.root).unwrap();

        // The global tree visitor will find all contract state hashes that will point us
        // to each and every contract's state
        let mut global_visitor = |node: &Node, path: &BitSlice<Msb0, u8>| match node {
            Node::Leaf(contract_state_hash) => {
                // Leaf value is the contract state hash for a particular contract address (which is the leaf path)
                // having a contract state hash we can get the contract state root
                // and traverse the entire contract state tree

                let contract_address = ContractAddress(StarkHash::from_bits(path).unwrap());
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
                let current_contract_global_storage = global_storage.entry(contract_address);

                // We use this visitor to inspect all the storage values for the current contract being processed in this very block
                let mut contract_visitor = |node: &Node, path: &BitSlice<Msb0, u8>| match node {
                    Node::Leaf(storage_value) => {
                        // Leaf value is the storage value for a particular storage key (which is the leaf path)

                        let storage_key = StorageAddress(StarkHash::from_bits(path).unwrap());
                        let storage_value = StorageValue(*storage_value);

                        match &current_contract_global_storage {
                            std::collections::hash_map::Entry::Occupied(
                                current_contract_all_diffs,
                            ) => {
                                let kvs_for_this_contract_all_diffs =
                                    current_contract_all_diffs.get();

                                match kvs_for_this_contract_all_diffs.get(&storage_key) {
                                    // This K-V pair is completely new
                                    None => {
                                        current_contract_delta.insert(storage_key, storage_value);
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
                                // Don't check in all_storage_diffs, this entire contract is a new entry globally
                                current_contract_delta.insert(storage_key, storage_value);
                            }
                        }
                    }
                    _ => {}
                };

                contract_state_tree.dfs(&mut contract_visitor);

                // Cleanup if it turned out that there were no updates to this contract in this block
                if current_contract_delta.is_empty() {
                    current_block_storage_delta.remove(&contract_address);
                }
            }
            _ => {}
        };

        global_tree.dfs(&mut global_visitor);

        let mut state_update = StateUpdate {
            block_hash: Some(block.hash),
            new_root: block.root,
            // FIXME old_root
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

        state_updates.push(state_update);
    }

    state_updates
}
