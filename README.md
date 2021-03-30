# rtdb

rtdb is a simple key/value database, based on prefix-compressed trie.

Supported operations:

 * Add a new key/value pair to the database.
 * Delete an existing key from the database.
 * Querying the database for a specific key.
 * Traverse all the data in the database.
 * Support for transactions.

example:

```c
struct rtdb *db = rtdb_open("./test.db");

struct rtdb_transaction *trans = rtdb_transaction_begin(db);
rtdb_put(trans, "apple", 5, "apple_value", 11);
rtdb_put(trans, "app", 3, "app_value", 9);
rtdb_put(trans, "application", 11, "application_value", 17);
rtdb_transaction_commit(trans);
//rtdb_transaction_rollback(trans);

rtdb_transaction_free(trans);
rtdb_close(db);
```

```c
struct rtdb *db = rtdb_open("./test.db");

struct rtdb_leaf *leaf = rtdb_get(db, "app", 3);
printf("app: %.*s\n", leaf->value_len, leaf->value);
rtdb_leaf_free(leaf);

rtdb_close(db);
```
