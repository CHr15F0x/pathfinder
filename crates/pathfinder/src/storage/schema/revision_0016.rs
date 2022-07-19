use anyhow::Context;
use rusqlite::Transaction;

/// Removes leaf nodes from `tree_global` and `tree_contracts` and
/// created `leaves_global` and `leaves_contracts` for faster lookup
/// instead.
pub(crate) fn migrate(transaction: &Transaction<'_>) -> anyhow::Result<()> {
    // TODO Should there be a foreign key in those tables?
    // TODO We don't delete stuff from the MPT, should we here..?
    transaction
        .execute_batch(
            r"
            CREATE TABLE leaves_global (
                root BLOB NOT NULL,
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY(root, key)
                FOREIGN KEY(root) REFERENCES starknet_blocks(root)
                -- FIXME We don't delete stuff from the MPT, should we here..?
                -- 
                -- ON DELETE CASCADE
            );
            ",
        )
        .context("Creating leaves_global table")?;

    transaction
        .execute_batch(
            r"
            CREATE TABLE leaves_contracts (
                root BLOB NOT NULL,
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY(root, key)
                FOREIGN KEY(root) REFERENCES contract_states(root)
                -- FIXME We don't delete stuff from the MPT, should we here..?
                -- 
                -- ON DELETE CASCADE
            );
            ",
        )
        .context("Creating leaves_global table")?;

    // let latest_block_number = crate::storage::StarknetBlocksTable::get_latest_number(transaction)
    //     .unwrap()
    //     .unwrap();

    use crate::state::{
        merkle_node::Node,
        state_tree::{ContractsStateTree, GlobalStateTree},
    };
    use crate::storage::{StarknetBlocksBlockId, StarknetBlocksTable};

    let root = StarknetBlocksTable::get_root(&transaction, StarknetBlocksBlockId::Latest)
        // TODO
        .unwrap()
        // Unwrap is safe because the DB cannot be empty at this point
        .unwrap();

    let global_tree = GlobalStateTree::load(transaction, root).unwrap();

    let mut contract_visitor = |node: &Node| {
        let persisted_node = match node {
            Node::Unresolved(_) => {
                unreachable!("Unresolved nodes are never visited but resolved instead")
            }
            Node::Binary(b) => {
                // merkle_tree::PersistedNode::Binary(merkle_tree::PersistedBinaryNode {
                //     left: b.left.borrow().hash().unwrap(),
                //     right: b.right.borrow().hash().unwrap(),
                // })
            }
            Node::Edge(e) => {
                //     merkle_tree::PersistedNode::Edge(merkle_tree::PersistedEdgeNode {
                //     path: e.path.clone(),
                //     child: e.child.borrow().hash().unwrap(),
                // })
            }
            Node::Leaf(_) => {
                // merkle_tree::PersistedNode::Leaf
            }
        };
        // contracts
        //     .insert(node.hash().unwrap(), persisted_node)
        //     .unwrap();
    };

    let mut global_visitor = |node: &Node| {
        let _persisted_node = match node {
            Node::Unresolved(_) => {
                unreachable!("Unresolved nodes are never visited but resolved instead")
            }
            Node::Binary(_b) => {
                // merkle_tree::PersistedNode::Binary(merkle_tree::PersistedBinaryNode {
                //     left: b.left.borrow().hash().unwrap(),
                //     right: b.right.borrow().hash().unwrap(),
                // })
            }
            Node::Edge(_e) => {
                //     merkle_tree::PersistedNode::Edge(merkle_tree::PersistedEdgeNode {
                //     path: e.path.clone(),
                //     child: e.child.borrow().hash().unwrap(),
                // })
            }
            Node::Leaf(contract_state_hash) => {
                // Leaf value is the contract state hash for a particular contract address (which is the leaf key)
                // having a contract state hash we can get the contract state root
                // and traverse the entire contract state tree
                let contract_state_root = ContractsStateTable::get_root(
                    &transaction,
                    pathfinder_lib::core::ContractStateHash(*contract_state_hash),
                )
                // FIXME
                .unwrap()
                // FIXME
                .unwrap();
                let contract_state_tree =
                    ContractsStateTree::load(&transaction, contract_state_root).unwrap();
                contract_state_tree.dfs(&mut contract_visitor);

                // merkle_tree::PersistedNode::Leaf
            }
        };
        // global.insert(node.hash().unwrap(), persisted_node).unwrap();
    };

    let global_tree = GlobalStateTree::load(&transaction, root).unwrap();
    global_tree.dfs(&mut global_visitor);

    Ok(())
}
