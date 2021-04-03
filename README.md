# ctdb

ctdb is a simple key/value database, based on prefix-compressed trie.

Storage is append-only, written datas are never changed.

Supported operations:

 * Add a new key/value pair to the database.
 * Delete an existing key from the database.
 * Querying the database for a specific key.
 * Traverse all the data in the database.
 * Support for transactions.

example:

```c
struct ctdb *db = ctdb_open("./test.db");

struct ctdb_transaction *trans = ctdb_transaction_begin(db);
ctdb_put(trans, "apple", 5, "apple_value", 11);
ctdb_put(trans, "app", 3, "app_value", 9);
ctdb_put(trans, "application", 11, "application_value", 17);
ctdb_transaction_commit(trans);
//ctdb_transaction_rollback(trans);

ctdb_transaction_free(trans);
ctdb_close(db);
```

```c
struct ctdb *db = ctdb_open("./test.db");

struct ctdb_leaf *leaf = ctdb_get(db, "app", 3);
printf("app: %.*s\n", leaf->value_len, leaf->value);
ctdb_leaf_free(leaf);

ctdb_close(db);
```
