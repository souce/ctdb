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

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "serializer.h"
#include "ctdb.h"

///////////////////////////////////////////////////////////////////////////////
// SERIALIZER
///////////////////////////////////////////////////////////////////////////////
static inline int check_header(int fd) {
    char magic_str[RTDB_MAGIC_LEN + 1] = {[0 ... RTDB_MAGIC_LEN] = 0};
    int version_num = -1;
    char header_buf[RTDB_HEADER_SIZE + 1] = {[0 ... RTDB_HEADER_SIZE] = 0}, *buf = header_buf, *end = &(header_buf[RTDB_HEADER_SIZE]);
    if (-1 == lseek(fd, 0, SEEK_SET)) return RTDB_ERR;  //get back to the beginning
    if (RTDB_HEADER_SIZE != read(fd, header_buf, RTDB_HEADER_SIZE)) return RTDB_ERR;
    if (SERIALIZER_OK != SERIALIZER_BUF_READ_STR(buf, end, magic_str, RTDB_MAGIC_LEN) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, version_num, uint32_t)) {
        return RTDB_ERR;
    }
    if (0 == strncmp(magic_str, RTDB_MAGIC_STR, RTDB_MAGIC_LEN) && RTDB_VERSION_NUM == version_num)
        return RTDB_OK;
    return RTDB_ERR;
}

static inline int dump_header(int fd) {
    char header_buf[RTDB_HEADER_SIZE + 1] = {[0 ... RTDB_HEADER_SIZE] = 0}, *buf = header_buf, *end = &(header_buf[RTDB_HEADER_SIZE]);
    if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_STR(buf, end, RTDB_MAGIC_STR, RTDB_MAGIC_LEN) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, RTDB_VERSION_NUM, uint32_t)) {
        return RTDB_ERR;
    }
    if (-1 == lseek(fd, 0, SEEK_SET)) return RTDB_ERR;  //get back to the beginning
    if (RTDB_HEADER_SIZE != write(fd, header_buf, RTDB_HEADER_SIZE)) return RTDB_ERR;
    return RTDB_OK;
}

#define FOOTER_ALIGNED(num) ({ ((num) + RTDB_FOOTER_ALIGNED_BASE - 1) & ~(RTDB_FOOTER_ALIGNED_BASE - 1); });

static inline int load_footer(int fd, struct ctdb_footer *footer) {
    off_t file_size = lseek(fd, -RTDB_FOOTER_ALIGNED_BASE, SEEK_END);
    off_t flag_aligned_pos = FOOTER_ALIGNED(file_size);  //find the right place for the 'transaction flag'
    while (flag_aligned_pos > RTDB_HEADER_SIZE + RTDB_FOOTER_ALIGNED_BASE) {
        int16_t cksum_1 = 1, cksum_2 = 2;
        char *buf = (char[RTDB_FOOTER_SIZE + 1]){[0 ... RTDB_FOOTER_SIZE] = 0}, *end = &(buf[RTDB_FOOTER_SIZE]);
        struct ctdb_footer footer_in_file = {.tran_count = 0, .del_count = 0, .root_pos = 0};
        if (-1 == lseek(fd, flag_aligned_pos, SEEK_SET)) goto retry;
        if (RTDB_FOOTER_SIZE != read(fd, buf, RTDB_FOOTER_SIZE)) goto retry;
        if (SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, cksum_1, int16_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, footer_in_file.tran_count, uint64_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, footer_in_file.del_count, uint64_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, footer_in_file.root_pos, uint64_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, cksum_2, int16_t)) {
            goto retry;
        }
        if (0 != cksum_1 && cksum_1 == cksum_2) {  //check the mark, make sure the data is correct
            *footer = footer_in_file;
            return RTDB_OK;
        }
    retry:
        flag_aligned_pos -= RTDB_FOOTER_ALIGNED_BASE;  //when searching for the 'transaction flag', it spans an alignment length at a time
        continue;
    }
    *footer = (struct ctdb_footer){.tran_count = 0, .del_count = 0, .root_pos = 0};
    return RTDB_ERR;
}

#define RANDOM_CKSUM() ({ int16_t min = 1, max = INT16_MAX; rand() % (max - min + 1) + min; })
static inline int dump_footer(int fd, struct ctdb_footer *footer) {
    char footer_buf[RTDB_FOOTER_SIZE + 1] = {[0 ... RTDB_FOOTER_SIZE] = 0}, *buf = footer_buf, *end = &(footer_buf[RTDB_FOOTER_SIZE]);
    int16_t cksum = RANDOM_CKSUM();
    if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, cksum, int16_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, footer->tran_count, uint64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, footer->del_count, uint64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, footer->root_pos, uint64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, cksum, int16_t)) {
        return RTDB_ERR;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    off_t flag_aligned_pos = FOOTER_ALIGNED(file_size);  //find a right position to write the 'transaction flag'
    if (-1 == lseek(fd, flag_aligned_pos, SEEK_SET)) return RTDB_ERR;
    if (RTDB_FOOTER_SIZE != write(fd, footer_buf, RTDB_FOOTER_SIZE)) return RTDB_ERR;
    return RTDB_OK;
}

static inline int load_items(int fd, int items_count, struct ctdb_node_item *items) {
    int item_size = items_count * RTDB_ITEMS_SIZE;
    char item_buf[item_size], *buf = item_buf, *end = &(item_buf[item_size - 1]);
    if (item_size != read(fd, item_buf, item_size)) return RTDB_ERR;
    int i = 0;
    for (; i < items_count; i++) {
        if (SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, items[i].sub_prefix_char, uint8_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, items[i].sub_node_pos, uint64_t)) {
            return RTDB_ERR;
        }
    }
    return RTDB_OK;
}
static int load_node(int fd, off_t node_pos, struct ctdb_node *node) {
    char *buf = (char[RTDB_NODE_SIZE + 1]){[0 ... RTDB_NODE_SIZE] = 0}, *end = &(buf[RTDB_NODE_SIZE]);
    if (-1 == lseek(fd, node_pos, SEEK_SET)) return RTDB_ERR;
    if (RTDB_NODE_SIZE != read(fd, buf, RTDB_NODE_SIZE)) return RTDB_ERR;
    if (SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, node->prefix_len, uint8_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_STR(buf, end, node->prefix, RTDB_MAX_KEY_LEN) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, node->leaf_pos, uint64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf, end, node->items_count, uint8_t)) {
        return RTDB_ERR;
    }
    if (RTDB_OK != load_items(fd, node->items_count, node->items)) return RTDB_ERR;
    return RTDB_OK;
}

static inline int dump_items(int fd, int items_count, struct ctdb_node_item *items) {
    int item_size = items_count * RTDB_ITEMS_SIZE;
    char item_buf[item_size], *buf = item_buf, *end = &(item_buf[item_size - 1]);
    int i = 0;
    for (; i < items_count; i++) {
        if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, items[i].sub_prefix_char, uint8_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, items[i].sub_node_pos, uint64_t)) {
            return RTDB_ERR;
        }
    }
    if (item_size != write(fd, item_buf, item_size)) return RTDB_ERR;
    return RTDB_OK;
}
static off_t dump_node(int fd, struct ctdb_node *node) {
    char node_buf[RTDB_NODE_SIZE + 1] = {[0 ... RTDB_NODE_SIZE] = 0}, *buf = node_buf, *end = &(node_buf[RTDB_NODE_SIZE]);
    if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, node->prefix_len, uint8_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_STR(buf, end, node->prefix, RTDB_MAX_KEY_LEN) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, node->leaf_pos, uint64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf, end, node->items_count, uint8_t)) {
        goto err;
    }
    off_t node_pos = lseek(fd, 0, SEEK_END);  //reach to the end
    if (-1 == node_pos) goto err;
    if (RTDB_NODE_SIZE != write(fd, node_buf, RTDB_NODE_SIZE)) goto err;
    if (RTDB_OK != dump_items(fd, node->items_count, node->items)) goto err;
    return node_pos;

err:
    return -1;
}

static int load_leaf(int fd, off_t leaf_pos, struct ctdb_leaf *leaf) {
    if (-1 == lseek(fd, leaf_pos, SEEK_SET)) goto err;
    if (SERIALIZER_OK != SERIALIZER_IO_READ_NUM(fd, leaf->value_len, uint32_t) ||
        SERIALIZER_OK != SERIALIZER_IO_READ_NEW_STR(fd, leaf->value, leaf->value_len)) {
        goto err;
    }
    return RTDB_OK;

err:
    if (NULL != leaf->value)
        free(leaf->value);
    leaf->value = NULL;
    leaf->value_len = 0;
    return RTDB_ERR;
}

static off_t dump_leaf(int fd, struct ctdb_leaf *leaf) {
    off_t leaf_pos = lseek(fd, 0, SEEK_END);  //reach to the end
    if (-1 == leaf_pos) goto err;
    if (SERIALIZER_OK != SERIALIZER_IO_WRITE_NUM(fd, leaf->value_len, uint32_t) ||
        SERIALIZER_OK != SERIALIZER_IO_WRITE_STR(fd, leaf->value, leaf->value_len)) {
        goto err;
    }
    return leaf_pos;

err:
    return -1;
}

///////////////////////////////////////////////////////////////////////////////
// COMMEN
///////////////////////////////////////////////////////////////////////////////
static inline int prefix_copy(char *filled_prefix_dst, uint8_t *prefix_dst_len, char *filled_prefix_src, int max_len) {
    //the prefix's length is "RTDB_MAX_KEY_LEN + 1", it's safe
    if (max_len <= RTDB_MAX_KEY_LEN && filled_prefix_dst == strncpy(filled_prefix_dst, filled_prefix_src, max_len)) {
        if (NULL != prefix_dst_len) *prefix_dst_len = strnlen(filled_prefix_dst, RTDB_MAX_KEY_LEN);
        return RTDB_OK;
    }
    return RTDB_ERR;
}

static int item_cmp(const void *a, const void *b) {
    const struct ctdb_node_item *i = a, *j = b;
    return i->sub_prefix_char - j->sub_prefix_char;
}

///////////////////////////////////////////////////////////////////////////////
// API
///////////////////////////////////////////////////////////////////////////////
struct ctdb *ctdb_open(char *path) {
    struct ctdb *db = NULL;
    int fd = -1;

    db = calloc(1, sizeof(*db));
    if (NULL == db) goto err;

    if (-1 == access(path, F_OK)) {
        fd = open(path, O_RDWR | O_CREAT, 0777);
        if (0 > fd) goto err;
        if (SERIALIZER_OK != dump_header(fd)) goto err;
    } else {
        fd = open(path, O_RDWR);
        if (0 > fd) goto err;
        if (SERIALIZER_OK != check_header(fd)) goto err;
        load_footer(fd, &(db->footer));  //try to find the last successful transaction, the old data will not be covered
    }
    db->fd = fd;
    return db;

err:
    if (NULL != db)
        free(db);
    if (0 <= fd)
        close(fd);
    return NULL;
}

void ctdb_close(struct ctdb *db) {
    if (NULL == db) return;
    if (0 <= db->fd)
        close(db->fd);
    free(db);
}

static off_t find_node_from_file(int fd, struct ctdb_node *trav, char *prefix, int prefix_len, int prefix_pos) {
    while (prefix_len > prefix_pos) {
        char prefix_char = prefix[prefix_pos];
        struct ctdb_node_item key_item = {.sub_prefix_char = prefix_char, .sub_node_pos = 0};
        struct ctdb_node_item *item = (struct ctdb_node_item *)bsearch(&key_item, trav->items, trav->items_count, sizeof(key_item), item_cmp);
        if (NULL == item) goto err;  //the item not found in child nodes, stop searching

        //load node from the file
        struct ctdb_node sub_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
        if (RTDB_OK != load_node(fd, item->sub_node_pos, &sub_node)) goto err;

        //across the same prefix
        int key_prefix_pos = prefix_pos;
        int sub_node_prefix_pos = 0;
        while (sub_node_prefix_pos < sub_node.prefix_len && key_prefix_pos < prefix_len && prefix[key_prefix_pos] == sub_node.prefix[sub_node_prefix_pos]) {
            ++key_prefix_pos;
            ++sub_node_prefix_pos;
        };

        if (sub_node_prefix_pos == sub_node.prefix_len) {
            //continue to traverse to the next node of the tree
            return find_node_from_file(fd, &sub_node, prefix, prefix_len, key_prefix_pos);
        }
        goto err;  //the current node's prefix does not match the key, stop searching
    }
    return trav->leaf_pos;

err:
    return 0;
}

struct ctdb_leaf *ctdb_get(struct ctdb *db, char *key, int key_len) {
    struct ctdb_leaf *leaf = NULL;
    if (0 >= key_len || RTDB_MAX_KEY_LEN < key_len || NULL == key) goto err;
    if (0 >= db->footer.root_pos) goto err;

    struct ctdb_node root = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
    if (RTDB_OK != load_node(db->fd, db->footer.root_pos, &root)) goto err;

    //search the prefix nodes related to key from the file
    char filled_prefix_key[RTDB_MAX_KEY_LEN + 1] = {[0 ... RTDB_MAX_KEY_LEN] = 0};
    if (filled_prefix_key != strncpy(filled_prefix_key, key, key_len)) goto err;
    off_t leaf_pos = find_node_from_file(db->fd, &root, filled_prefix_key, key_len, 0);
    if (0 >= leaf_pos) goto err;  //no data found

    leaf = calloc(1, sizeof(*leaf));
    if (NULL == leaf || RTDB_OK != load_leaf(db->fd, leaf_pos, leaf)) goto err;
    if (0 == leaf->value_len) goto err;  //the data has been deleted
    return leaf;

err:
    ctdb_leaf_free(leaf);
    return NULL;
}

void ctdb_leaf_free(struct ctdb_leaf *leaf){
    if(NULL != leaf){
        if(NULL != leaf->value){
            free(leaf->value);
        }
        free(leaf);
    }
}

struct ctdb_transaction *ctdb_transaction_begin(struct ctdb *db) {
    struct ctdb_transaction *trans = calloc(1, sizeof(*trans));
    if (NULL != trans) {
        trans->is_isvalid = 1;
        trans->db = db;
        trans->new_footer = db->footer;
    }
    return trans;
}

static void put_node_in_items(struct ctdb_node *father_node, char sub_prefix_char, int sub_node_pos) {
    struct ctdb_node_item new_item = {.sub_prefix_char = sub_prefix_char, .sub_node_pos = sub_node_pos};
    struct ctdb_node_item *exists_item = (struct ctdb_node_item *)bsearch(&new_item, father_node->items, father_node->items_count, sizeof(new_item), item_cmp);
    if (exists_item != NULL) {
        exists_item->sub_node_pos = sub_node_pos;
    } else {
        father_node->items[father_node->items_count++] = new_item; //as long as the 'RTDB_MAX_CHAR_RANGE' is 256, it will not out of the bounds
        qsort(father_node->items, father_node->items_count, sizeof(struct ctdb_node_item), item_cmp);
    }
}

static off_t save_node_into_file(int fd, struct ctdb_node *trav, char *prefix, int prefix_len, int prefix_pos, off_t leaf_pos) {
    while (prefix_len > prefix_pos) {
        char prefix_char = prefix[prefix_pos];
        struct ctdb_node_item key_item = {.sub_prefix_char = prefix_char, .sub_node_pos = 0};
        struct ctdb_node_item *item = (struct ctdb_node_item *)bsearch(&key_item, trav->items, trav->items_count, sizeof(key_item), item_cmp);
        if (NULL == item) break;  //the item not found in child nodes, stop searching

        //load node from the file
        struct ctdb_node sub_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
        if (RTDB_OK != load_node(fd, item->sub_node_pos, &sub_node)) goto err;

        //across the same prefix
        int key_prefix_pos = prefix_pos;
        int sub_node_prefix_pos = 0;
        while (sub_node_prefix_pos < sub_node.prefix_len && key_prefix_pos < prefix_len && 
                prefix[key_prefix_pos] == sub_node.prefix[sub_node_prefix_pos]) {
            ++key_prefix_pos;
            ++sub_node_prefix_pos;
        };

        if (sub_node_prefix_pos == sub_node.prefix_len) {
            //continue to traverse to the next node of the tree
            off_t new_node_pos = save_node_into_file(fd, &sub_node, prefix, prefix_len, key_prefix_pos, leaf_pos);
            if (0 >= new_node_pos) goto err;
            put_node_in_items(trav, sub_node.prefix[0], new_node_pos);
            return dump_node(fd, trav);  //append the node to the end of file
        } else {
            char old_remained[RTDB_MAX_KEY_LEN + 1] = {[0 ... RTDB_MAX_KEY_LEN] = 0};  //the old prefix does not include duplicate parts
            if (RTDB_OK != prefix_copy(old_remained, NULL, sub_node.prefix + sub_node_prefix_pos, RTDB_MAX_KEY_LEN)) goto err;
            char new_remained[RTDB_MAX_KEY_LEN + 1] = {[0 ... RTDB_MAX_KEY_LEN] = 0};  //the new prefix does not include duplicate parts
            if (RTDB_OK != prefix_copy(new_remained, NULL, prefix + key_prefix_pos, RTDB_MAX_KEY_LEN)) goto err;
            if (key_prefix_pos == prefix_len) {
                //if the new prefix has been traversed, the old prefix is longer and the new node should be inserted before the old node
                if (RTDB_OK != prefix_copy(sub_node.prefix, &sub_node.prefix_len, old_remained, RTDB_MAX_KEY_LEN)) goto err;
                off_t sub_node_pos = dump_node(fd, &sub_node);  //append the node to the end of file
                if (0 >= sub_node_pos) goto err;

                //old node as a child of new node
                struct ctdb_node new_node = {.prefix_len = 0, .leaf_pos = leaf_pos, .items_count = 0};
                if (RTDB_OK != prefix_copy(new_node.prefix, &new_node.prefix_len, prefix + prefix_pos, RTDB_MAX_KEY_LEN)) goto err;
                put_node_in_items(&new_node, sub_node.prefix[0], sub_node_pos);
                off_t new_node_pos = dump_node(fd, &new_node);  //append the node to the end of file
                if (0 >= new_node_pos) goto err;

                put_node_in_items(trav, new_node.prefix[0], new_node_pos);
                return dump_node(fd, trav);  //append the node to the end of file
            } else {
                //the new prefix and the old prefix are not duplicate, split a common node to accommodate both
                struct ctdb_node temp_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
                if (RTDB_OK != prefix_copy(temp_node.prefix, &temp_node.prefix_len, sub_node.prefix, sub_node_prefix_pos)) goto err;

                if (RTDB_OK != prefix_copy(sub_node.prefix, &sub_node.prefix_len, old_remained, RTDB_MAX_KEY_LEN)) goto err;
                off_t sub_node_pos = dump_node(fd, &sub_node);  //append the node to the end of file
                if (0 >= sub_node_pos) goto err;
                put_node_in_items(&temp_node, sub_node.prefix[0], sub_node_pos);

                struct ctdb_node new_node = {.prefix_len = 0, .leaf_pos = leaf_pos, .items_count = 0};
                if (RTDB_OK != prefix_copy(new_node.prefix, &new_node.prefix_len, new_remained, RTDB_MAX_KEY_LEN)) goto err;
                off_t new_node_pos = dump_node(fd, &new_node);  //append the node to the end of file
                if (0 >= new_node_pos) goto err;
                put_node_in_items(&temp_node, new_node.prefix[0], new_node_pos);

                off_t temp_node_pos = dump_node(fd, &temp_node);  //append the node to the end of file
                put_node_in_items(trav, temp_node.prefix[0], temp_node_pos);
                return dump_node(fd, trav);  //append the node to the end of file
            }
        }
    }  //end:while
    if (prefix_pos < prefix_len) {
        //initialize the new node, or the new prefix is longer than the old prefix
        struct ctdb_node new_node = {.prefix_len = 0, .leaf_pos = leaf_pos, .items_count = 0};
        if (RTDB_OK != prefix_copy(new_node.prefix, &new_node.prefix_len, prefix + prefix_pos, RTDB_MAX_KEY_LEN)) goto err;
        off_t new_node_pos = dump_node(fd, &new_node);  //append the node to the end of file
        if (0 >= new_node_pos) goto err;
        put_node_in_items(trav, new_node.prefix[0], new_node_pos);
        return dump_node(fd, trav);  //append the node to the end of file
    } else {
        //duplicate prefix, replace
        trav->leaf_pos = leaf_pos;
        return dump_node(fd, trav);  //append the node to the end of file
    }

err:
    return 0;
}

int ctdb_put(struct ctdb_transaction *trans, char *key, int key_len, char *value, int value_len) {
    if (NULL == trans || 1 != trans->is_isvalid) goto err;  //verify that the transaction has not been committed or rolled back
    if (0 >= key_len || RTDB_MAX_KEY_LEN < key_len || NULL == key) goto err;
    if (0 > value_len || RTDB_MAX_VALUE_LEN < value_len || NULL == value) goto err;  //delete value (whether it exists or not)

    struct ctdb_node root = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
    if (0 < trans->new_footer.root_pos) {
        if (RTDB_OK != load_node(trans->db->fd, trans->new_footer.root_pos, &root)) goto err;
    }

    //append the leaf node (value) to the file
    struct ctdb_leaf new_leaf = (struct ctdb_leaf){.value_len = value_len, .value = value};
    off_t new_leaf_pos = dump_leaf(trans->db->fd, &new_leaf);
    if (0 >= new_leaf_pos) goto err;

    //update the prefix nodes
    char filled_prefix_key[RTDB_MAX_KEY_LEN + 1] = {[0 ... RTDB_MAX_KEY_LEN] = 0};
    if (filled_prefix_key != strncpy(filled_prefix_key, key, key_len)) goto err;
    trans->new_footer.root_pos = save_node_into_file(trans->db->fd, &root, filled_prefix_key, key_len, 0, new_leaf_pos);
    if (0 >= trans->new_footer.root_pos) goto err;

    //cumulative the operation count
    trans->new_footer.tran_count += 1;
    if (0 >= value_len || NULL == value)
        trans->new_footer.del_count += 1;
    
    return RTDB_OK;

err:
    return RTDB_ERR;
}

int ctdb_del(struct ctdb_transaction *trans, char *key, int key_len) {
    return ctdb_put(trans, key, key_len, "", 0);
}

int ctdb_transaction_commit(struct ctdb_transaction *trans) {
    if (NULL == trans || 1 != trans->is_isvalid) goto err;  //verify that the transaction has not been committed or rolled back
    trans->is_isvalid = 0;  //the transaction that have been used (commit, rollback) cannot be used any more

    struct ctdb *db = trans->db;
    //save the 'transaction flag', which means that the transaction was committed successfully
    if (RTDB_OK != dump_footer(db->fd, &(trans->new_footer))) goto err;
    if (-1 == fsync(db->fd)) goto err;
    db->footer = trans->new_footer;
    return RTDB_OK;

err:
    return RTDB_ERR;
}

void ctdb_transaction_rollback(struct ctdb_transaction *trans) {
    if (NULL != trans) {
        trans->is_isvalid = 0;  //the transaction that have been used (commit, rollback) cannot be used any more
    }
}

void ctdb_transaction_free(struct ctdb_transaction *trans){
    if (NULL != trans) {
        free(trans);
    }
}

///////////////////////////////////////////////////////////////////////////////
// iterator
///////////////////////////////////////////////////////////////////////////////
static int iterator_travel(int fd, struct ctdb_node *trav, char *key, int key_len, ctdb_traversal *traversal) {
    if ((trav->prefix_len + key_len) < RTDB_MAX_KEY_LEN) {
        char prefix_key[RTDB_MAX_KEY_LEN + 1] = {[0 ... RTDB_MAX_KEY_LEN] = 0};
        if (0 > sprintf(prefix_key, "%.*s%.*s", key_len, key, trav->prefix_len, trav->prefix)) goto over;
        int prefix_key_len = key_len + trav->prefix_len;

        if (0 < trav->leaf_pos) {
            struct ctdb_leaf leaf = {.value_len = 0, .value = NULL};
            if (RTDB_OK != load_leaf(fd, trav->leaf_pos, &leaf)) goto over;
            if (0 < prefix_key_len && 0 < leaf.value_len) {                              //the data has not been deleted
                if (RTDB_OK != traversal(prefix_key, prefix_key_len, &leaf)) goto over;  //the traversal operation has been cancelled
            }
            free(leaf.value);
        }

        int items_index = 0;
        for (; items_index < trav->items_count; items_index++) {
            off_t sub_node_pos = trav->items[items_index].sub_node_pos;
            struct ctdb_node sub_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
            if (RTDB_OK != load_node(fd, sub_node_pos, &sub_node)) goto over;
            if (RTDB_OK != iterator_travel(fd, &sub_node, prefix_key, prefix_key_len, traversal)) goto over;  //something wrong, or the traversal operation has been cancelled
        }
    }
    return RTDB_OK;

over:
    return RTDB_ERR;
}

int ctdb_iterator_travel(struct ctdb *db, ctdb_traversal *traversal) {
    struct ctdb_node root = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
    if (RTDB_OK == load_node(db->fd, db->footer.root_pos, &root)) {
        return iterator_travel(db->fd, &root, "", 0, traversal);
    }
    return RTDB_ERR;
}
