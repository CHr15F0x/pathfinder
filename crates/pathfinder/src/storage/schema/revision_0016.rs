use anyhow::Context;
use rusqlite::Transaction;

use crate::core::StarknetBlockNumber;

/// Removes leaf nodes from `tree_global` and `tree_contracts` and
/// created `leaves_global` and `leaves_contracts` for faster lookup
/// instead.
pub(crate) fn migrate(transaction: &Transaction<'_>) -> anyhow::Result<()> {
    // TODO Should there be a foreign key in those tables?
    // TODO We don't delete stuff from the MPT, should we here..?
    transaction
        .execute_batch(
            // -- FOREIGN KEY(root) REFERENCES starknet_blocks(root)
            // -- FIXME We don't delete stuff from the MPT, should we here..?
            // --
            // -- ON DELETE CASCADE
            r"
            CREATE TABLE leaves_global (
                root BLOB NOT NULL,
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY(root, key)
            );

            CREATE TABLE leaves_contracts (
                root BLOB NOT NULL,
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY(root, key)
            );
            ",
        )
        .context("Creating leaves_global and leaves_contracts tables")?;

    use crate::core::ContractStateHash;
    use crate::state::{
        merkle_node::Node,
        state_tree::{ContractsStateTree, GlobalStateTree},
    };
    use crate::storage::{ContractsStateTable, StarknetBlocksBlockId, StarknetBlocksTable};
    use rusqlite::params;
    use stark_hash::StarkHash;

    let latest_block_number = StarknetBlocksTable::get_latest_number(transaction)
        // FIXME
        // .unwrap()
        // FIXME?
        .unwrap();

    let latest_block_number = match latest_block_number {
        Some(n) => n,
        None => {
            // Migration finished
            return Ok(());
        }
    };

    for block_number in (2788..=latest_block_number.0).rev() {
        // FIXME
        println!(
            "Processing block {block_number}/{}",
            latest_block_number.0 + 1
        );

        let global_root = StarknetBlocksTable::get_root(
            &transaction,
            StarknetBlocksBlockId::Number(StarknetBlockNumber(block_number)),
        )
        // FIXME
        .unwrap()
        // FIXME?
        // Unwrap is safe because the DB cannot be empty at this point
        .unwrap();

        let mut global_visitor = |node: &Node, path_is_key_for_leaf: StarkHash| {
            match node {
                Node::Leaf(contract_state_hash) => {
                    transaction
                        .execute(
                            r"INSERT INTO leaves_global (root, key, value) VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING",
                            params![
                                &global_root.0.as_be_bytes()[..],
                                &path_is_key_for_leaf.as_be_bytes()[..],
                                &contract_state_hash.as_be_bytes()[..],
                            ],
                        )
                        // FIXME
                        .unwrap();

                    // Leaf value is the contract state hash for a particular contract address (which is the leaf key)
                    // having a contract state hash we can get the contract state root
                    // and traverse the entire contract state tree
                    let contract_state_root = ContractsStateTable::get_root(
                        &transaction,
                        ContractStateHash(*contract_state_hash),
                    )
                    // FIXME
                    .unwrap()
                    // FIXME
                    .unwrap();

                    let mut contract_visitor = |node: &Node, path_is_key_for_leaf: StarkHash| {
                        match node {
                            Node::Leaf(value) => {
                                transaction
                            .execute(
                                r"INSERT INTO leaves_contracts (root, key, value) VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING",
                                params![
                                    &contract_state_root.0.as_be_bytes()[..],
                                    &path_is_key_for_leaf.as_be_bytes()[..],
                                    &value.as_be_bytes()[..],
                                ],
                            )
                            // FIXME
                            .unwrap();
                            }
                            _ => {}
                        };
                    };

                    let contract_state_tree =
                        ContractsStateTree::load(&transaction, contract_state_root).unwrap();
                    contract_state_tree.dfs(&mut contract_visitor);
                }
                _ => {}
            };
        };

        let global_tree = GlobalStateTree::load(&transaction, global_root).unwrap();
        global_tree.dfs(&mut global_visitor);
    }

    Ok(())
}
