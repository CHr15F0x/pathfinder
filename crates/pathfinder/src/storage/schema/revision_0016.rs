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

    Ok(())
}
