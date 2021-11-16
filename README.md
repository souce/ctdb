# ctdb

ctdb is a simple & lightweight key-value database, based on Append-only Compressed Trie.

### Supported operations

 * Add a new key/value pair to the database.
 * Delete an existing key from the database.
 * Querying the database by a specific key.
 * Traverse the database by a specific prefix.
 * Support for transactions.

### Quick start

```
make iter; ./iter

make simple; ./simple

make trans; rm ./test.db; ./trans

make vacuum; rm ./*.db; ./vacuum
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

struct ctdb_transaction *trans = ctdb_transaction_begin(db);
struct ctdb_leaf leaf = ctdb_get(trans, "app", 3);
//seek(db->fd, leaf.value_pos, SEEK_SET)
//read(db->fd, buffer, leaf.value_len)) or sendfile(client_fd, db->fd, &off, leaf.value_len)

ctdb_transaction_free(trans);
ctdb_close(db);
```

traverse:

```c
struct ctdb *db = ctdb_open("./test.db");
struct ctdb_transaction *trans = ctdb_transaction_begin(db);

//traverse callback
int traversal(char *key, int key_len, struct ctdb_leaf leaf){
    printf("key:%.*s value_len:%u\n", key_len, key, leaf->value_len);
    //return CTDB_ERR; //stop traversal
    return CTDB_OK; //continue
}

//traverse all the data
if(CTDB_OK == ctdb_iterator_travel(trans, "", 0, traversal)){
    printf("end of traversal\n");
}

//traversing data starting with "app"
if(CTDB_OK == ctdb_iterator_travel(trans, "app", 3, traversal)){
    printf("end of traversal\n");
}

ctdb_transaction_free(trans);
ctdb_close(db);
```

vacuum:

```c
struct ctdb *db = ctdb_open("./test.db");
struct ctdb_transaction *trans = ctdb_transaction_begin(db);
//...
//copy the data to a new file and compress it
struct ctdb *new_db = ctdb_open("./test_tmp.db");
assert(CTDB_OK == ctdb_vacuum(trans, new_db));
ctdb_transaction_free(&trans);
ctdb_close(&new_db);
ctdb_close(&db);
```
