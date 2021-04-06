# ctdb

ctdb is a simple key/value database, based on compressed trie.

Storage is append-only, written datas are never changed.

### Supported operations

 * Add a new key/value pair to the database.
 * Delete an existing key from the database.
 * Querying the database by a specific key.
 * Traverse the database by a specific prefix.
 * Support for transactions.

### Quick start

```
make simple; ./simple

make trans; ./trans

make iter; ./iter
```

### example

put & del:

```c
struct ctdb *db = ctdb_open("./test.db");

struct ctdb_transaction *trans = ctdb_transaction_begin(db);
ctdb_put(trans, "apple", 5, "apple_value", 11);
ctdb_put(trans, "app", 3, "app_value", 9);
ctdb_put(trans, "application", 11, "application_value", 17);

ctdb_del(trans, "app", 3);

ctdb_transaction_commit(trans);
//ctdb_transaction_rollback(trans);

ctdb_transaction_free(trans);
ctdb_close(db);
```

get:

```c
struct ctdb *db = ctdb_open("./test.db");

struct ctdb_leaf *leaf = ctdb_get(db, "app", 3);
printf("app: %.*s\n", leaf->value_len, leaf->value);
ctdb_leaf_free(leaf);

ctdb_close(db);
```

traverse:

```c
struct ctdb *db = ctdb_open("./test.db");

//traverse callback
int traversal(char *key, int key_len, struct ctdb_leaf *leaf){
    printf("key:%.*s value:%.*s\n", key_len, key, leaf->value_len, leaf->value);
    //return CTDB_ERR; //stop traversal
    return CTDB_OK; //continue
}

//traverse all the data
if(CTDB_OK == ctdb_iterator_travel(db, "", 0, traversal)){
    printf("end of traversal\n");
}


//traversing data starting with "app"
if(CTDB_OK == ctdb_iterator_travel(db, "app", 3, traversal)){
    printf("end of traversal\n");
}

ctdb_close(db);
```
