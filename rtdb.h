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
#define RTDB_MAGIC_STR "rtdb"
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

struct rtdb_footer{
    uint64_t tran_count;
    uint64_t del_count;
    off_t root_pos;
};

struct rtdb{
    int fd;
    struct rtdb_footer footer;
};

struct rtdb_node{
    uint8_t prefix_len;
    char prefix[RTDB_MAX_KEY_LEN + 1];
    off_t leaf_pos;
    
    uint8_t items_count;
    struct rtdb_node_item{
        char sub_prefix_char;
        off_t sub_node_pos;
    }items[RTDB_MAX_CHAR_RANGE];
};

struct rtdb_leaf{
    //int create_time;
    //int expire;
    //int fingerprint;
    uint32_t value_len;
    char *value;
};

struct rtdb_transaction{
    uint8_t is_isvalid;
    struct rtdb *db;
    struct rtdb_footer new_footer;
};

//API
struct rtdb *rtdb_open(char *path);
struct rtdb_leaf *rtdb_get(struct rtdb *db, char *key, int key_len);
void rtdb_leaf_free(struct rtdb_leaf *leaf);
struct rtdb_transaction *rtdb_transaction_begin(struct rtdb *db);
int rtdb_put(struct rtdb_transaction *trans, char *key, int key_len, char *value, int value_len);
int rtdb_del(struct rtdb_transaction *trans, char *key, int key_len);
int rtdb_transaction_commit(struct rtdb_transaction *trans);
void rtdb_transaction_rollback(struct rtdb_transaction *trans);
void rtdb_transaction_free(struct rtdb_transaction *trans);
void rtdb_close(struct rtdb *db);

//iterator
typedef int rtdb_traversal(char *key, int key_len, struct rtdb_leaf *leaf);
int rtdb_iterator_travel(struct rtdb *db, rtdb_traversal *traversal);

#ifdef __cplusplus
}
#endif
#endif
