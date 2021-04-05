/*
 * 
 * Copyright (c) 2021, Joel
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef __RTDB_H_
#define __RTDB_H_
#ifdef __cplusplus
extern "C" {
#endif

#define RTDB_HEADER_SIZE 128
#define RTDB_MAGIC_STR "ctdb"
#define RTDB_MAGIC_LEN 4
#define RTDB_VERSION_NUM 1

#define RTDB_MAX_KEY_LEN 64
#define RTDB_MAX_CHAR_RANGE 256
#define RTDB_MAX_VALUE_LEN (1024 * 1024 * 1024) //1G

#define RTDB_ITEMS_SIZE (1 + 8)
#define RTDB_NODE_SIZE (1 + RTDB_MAX_KEY_LEN + 8 + 1) //"items" not included!
//#define RTDB_LEAF_SIZE (4) 

#define RTDB_FOOTER_ALIGNED_BASE (32)
#define RTDB_FOOTER_SIZE (2 + 8 + 8 + 8 + 2)

#define RTDB_OK 0
#define RTDB_ERR -1

struct ctdb_footer{
    uint64_t tran_count;
    uint64_t del_count;
    off_t root_pos;
};

struct ctdb{
    int fd;
    struct ctdb_footer footer;
};

struct ctdb_node{
    uint8_t prefix_len;
    char prefix[RTDB_MAX_KEY_LEN + 1];
    off_t leaf_pos;
    
    uint8_t items_count;
    struct ctdb_node_item{
        char sub_prefix_char;
        off_t sub_node_pos;
    }items[RTDB_MAX_CHAR_RANGE];
};

struct ctdb_leaf{
    //int create_time;
    //int expire;
    //int fingerprint;
    uint32_t value_len;
    char *value;
};

struct ctdb_transaction{
    uint8_t is_isvalid;
    struct ctdb *db;
    struct ctdb_footer new_footer;
};

//API
struct ctdb *ctdb_open(char *path);
struct ctdb_leaf *ctdb_get(struct ctdb *db, char *key, int key_len);
void ctdb_leaf_free(struct ctdb_leaf *leaf);
struct ctdb_transaction *ctdb_transaction_begin(struct ctdb *db);
int ctdb_put(struct ctdb_transaction *trans, char *key, int key_len, char *value, int value_len);
int ctdb_del(struct ctdb_transaction *trans, char *key, int key_len);
int ctdb_transaction_commit(struct ctdb_transaction *trans);
void ctdb_transaction_rollback(struct ctdb_transaction *trans);
void ctdb_transaction_free(struct ctdb_transaction *trans);
void ctdb_close(struct ctdb *db);

//iterator
typedef int ctdb_traversal(char *key, int key_len, struct ctdb_leaf *leaf);
//int ctdb_iterator_travel(struct ctdb *db, ctdb_traversal *traversal);
int ctdb_iterator_travel(struct ctdb *db, char *key, int key_len, ctdb_traversal *traversal);

#ifdef __cplusplus
}
#endif
#endif
