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
static int check_header(int fd) {
    char magic_str[CTDB_MAGIC_LEN + 1] = {[0 ... CTDB_MAGIC_LEN] = 0};
    int version_num = -1;
    char header_buf[CTDB_HEADER_SIZE + 1] = {[0 ... CTDB_HEADER_SIZE] = 0};
    char *buf_cur = header_buf, *buf_end = header_buf + CTDB_HEADER_SIZE;
    if (-1 == lseek(fd, 0, SEEK_SET)) return CTDB_ERR;  //get back to the beginning
    if (CTDB_HEADER_SIZE != read(fd, header_buf, CTDB_HEADER_SIZE)) return CTDB_ERR;
    if (SERIALIZER_OK != SERIALIZER_BUF_READ_STR(buf_cur, buf_end, magic_str, CTDB_MAGIC_LEN) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, version_num, uint32_t)) {
        return CTDB_ERR;
    }
    if (0 == strncmp(magic_str, CTDB_MAGIC_STR, CTDB_MAGIC_LEN) && CTDB_VERSION_NUM == version_num)
        return CTDB_OK;
    return CTDB_ERR;
}

static int dump_header(int fd) {
    char header_buf[CTDB_HEADER_SIZE + 1] = {[0 ... CTDB_HEADER_SIZE] = 0};
    char *buf_cur = header_buf, *buf_end = header_buf + CTDB_HEADER_SIZE;
    if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_STR(buf_cur, buf_end, CTDB_MAGIC_STR, CTDB_MAGIC_LEN) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, CTDB_VERSION_NUM, uint32_t)) {
        return CTDB_ERR;
    }
    if (-1 == lseek(fd, 0, SEEK_SET)) return CTDB_ERR;  //get back to the beginning
    if (CTDB_HEADER_SIZE != write(fd, header_buf, CTDB_HEADER_SIZE)) return CTDB_ERR;
    return CTDB_OK;
}

#define FOOTER_ALIGNED(num) ({ ((num) + CTDB_FOOTER_ALIGNED_BASE - 1) & ~(CTDB_FOOTER_ALIGNED_BASE - 1); });

static int load_footer(int fd, struct ctdb_footer *footer) {
    off_t file_size = lseek(fd, -CTDB_FOOTER_ALIGNED_BASE, SEEK_END);
    off_t flag_aligned_pos = FOOTER_ALIGNED(file_size);  //find the right place for the 'transaction flag'
    while (flag_aligned_pos >= CTDB_HEADER_SIZE) {
        uint64_t cksum_1 = 1, cksum_2 = 2;
        struct ctdb_footer footer_in_file = {.tran_count = 0, .del_count = 0, .root_pos = 0};
        char footer_buf[CTDB_FOOTER_SIZE + 1] = {[0 ... CTDB_FOOTER_SIZE] = 0};
        char *buf_cur = footer_buf, *buf_end = footer_buf + CTDB_FOOTER_SIZE;
        if (-1 == lseek(fd, flag_aligned_pos, SEEK_SET)) goto retry;
        if (CTDB_FOOTER_SIZE != read(fd, footer_buf, CTDB_FOOTER_SIZE)) goto retry;
        if (SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, cksum_1, uint64_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, footer_in_file.tran_count, uint64_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, footer_in_file.del_count, uint64_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, footer_in_file.root_pos, int64_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, cksum_2, uint64_t)) {
            goto retry;
        }
        //check the mark, make sure the data is correct (CheckSum)
        if (0 < cksum_1 && cksum_1 == cksum_2 && 
            file_size > footer_in_file.root_pos &&
            0 == 1 + cksum_2 + (footer_in_file.tran_count + footer_in_file.del_count + footer_in_file.root_pos)) {
            *footer = footer_in_file;
            return CTDB_OK;
        }
    retry:
        flag_aligned_pos -= CTDB_FOOTER_ALIGNED_BASE;  //when searching for the 'transaction flag', it spans an alignment length at a time
        continue;
    }
    *footer = (struct ctdb_footer){.tran_count = 0, .del_count = 0, .root_pos = 0};
    return CTDB_ERR;
}

static int dump_footer(int fd, struct ctdb_footer *footer) {
    char footer_buf[CTDB_FOOTER_SIZE + 1] = {[0 ... CTDB_FOOTER_SIZE] = 0};
    char *buf_cur = footer_buf, *buf_end = footer_buf + CTDB_FOOTER_SIZE;
    uint64_t cksum = ~(footer->tran_count + footer->del_count + footer->root_pos);  //CheckSum
    if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, cksum, uint64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, footer->tran_count, uint64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, footer->del_count, uint64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, footer->root_pos, int64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, cksum, uint64_t)) {
        return CTDB_ERR;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    off_t flag_aligned_pos = FOOTER_ALIGNED(file_size);  //find a right position to write the 'transaction flag'
    if (-1 == lseek(fd, flag_aligned_pos, SEEK_SET)) return CTDB_ERR;
    if (CTDB_FOOTER_SIZE != write(fd, footer_buf, CTDB_FOOTER_SIZE)) return CTDB_ERR;
    return CTDB_OK;
}

static inline off_t append_to_end(int fd, char *buf, uint32_t buf_len){
    off_t pos = lseek(fd, 0, SEEK_END);  //reach to the end
    if (-1 == pos) goto err;
    if (buf_len != write(fd, buf, buf_len)) goto err;
    return pos;

err:
    return -1;
}

static inline int load_items(int fd, int items_count, struct ctdb_node_item *items) {
    int item_size = items_count * CTDB_ITEMS_SIZE;
    char item_buf[item_size + 1];
    char *buf_cur = item_buf, *buf_end = item_buf + item_size;
    if (item_size != read(fd, item_buf, item_size)) return CTDB_ERR;
    int i = 0;
    for (; i < items_count; i++) {
        if (SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, items[i].sub_prefix_char, uint8_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, items[i].sub_node_pos, int64_t)) {
            return CTDB_ERR;
        }
    }
    return CTDB_OK;
}
static int load_node(int fd, off_t node_pos, struct ctdb_node *node) {
    char node_buf[CTDB_NODE_SIZE + 1] = {[0 ... CTDB_NODE_SIZE] = 0};
    char *buf_cur = node_buf, *buf_end = node_buf + CTDB_NODE_SIZE;
    if (-1 == lseek(fd, node_pos, SEEK_SET)) return CTDB_ERR;
    if (CTDB_NODE_SIZE != read(fd, node_buf, CTDB_NODE_SIZE)) return CTDB_ERR;
    if (SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, node->prefix_len, uint8_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_STR(buf_cur, buf_end, node->prefix, CTDB_MAX_KEY_LEN) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, node->leaf_pos, int64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, node->items_count, uint8_t)) {
        return CTDB_ERR;
    }
    if (CTDB_OK != load_items(fd, node->items_count, node->items)) return CTDB_ERR;
    return CTDB_OK;
}

static inline int dump_items(int fd, int items_count, struct ctdb_node_item *items) {
    int item_size = items_count * CTDB_ITEMS_SIZE;
    char item_buf[item_size + 1];
    char *buf_cur = item_buf, *buf_end = item_buf + item_size;
    int i = 0;
    for (; i < items_count; i++) {
        if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, items[i].sub_prefix_char, uint8_t) ||
            SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, items[i].sub_node_pos, int64_t)) {
            return CTDB_ERR;
        }
    }
    if (item_size != write(fd, item_buf, item_size)) return CTDB_ERR;
    return CTDB_OK;
}
static off_t dump_node(int fd, struct ctdb_node *node) {
    char node_buf[CTDB_NODE_SIZE + 1] = {[0 ... CTDB_NODE_SIZE] = 0};
    char *buf_cur = node_buf, *buf_end = node_buf + CTDB_NODE_SIZE;
    if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, node->prefix_len, uint8_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_STR(buf_cur, buf_end, node->prefix, CTDB_MAX_KEY_LEN) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, node->leaf_pos, int64_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, node->items_count, uint8_t)) {
        goto err;
    }
    off_t node_pos = append_to_end(fd, node_buf, CTDB_NODE_SIZE);
    if (0 >= node_pos) goto err;
    if (CTDB_OK != dump_items(fd, node->items_count, node->items)) goto err;
    return node_pos;

err:
    return -1;
}

static int load_leaf(int fd, off_t leaf_pos, struct ctdb_leaf *leaf) {
    char leaf_buf[CTDB_LEAF_SIZE + 1] = {[0 ... CTDB_LEAF_SIZE] = 0};
    char *buf_cur = leaf_buf, *buf_end = leaf_buf + CTDB_LEAF_SIZE;
    if (-1 == lseek(fd, leaf_pos, SEEK_SET)) return CTDB_ERR;
    if (CTDB_LEAF_SIZE != read(fd, leaf_buf, CTDB_LEAF_SIZE)) return CTDB_ERR;
    if (SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, leaf->value_len, uint32_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_READ_NUM(buf_cur, buf_end, leaf->value_pos, int64_t)) {
        return CTDB_ERR;
    }
    return CTDB_OK;
}

static off_t dump_leaf(int fd, struct ctdb_leaf *leaf) {
    char leaf_buf[CTDB_LEAF_SIZE + 1] = {[0 ... CTDB_LEAF_SIZE] = 0};
    char *buf_cur = leaf_buf, *buf_end = leaf_buf + CTDB_LEAF_SIZE;
    if (SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, leaf->value_len, uint32_t) ||
        SERIALIZER_OK != SERIALIZER_BUF_WRITE_NUM(buf_cur, buf_end, leaf->value_pos, int64_t)) {
        goto err;
    }
    off_t leaf_pos = append_to_end(fd, leaf_buf, CTDB_LEAF_SIZE);
    if (0 >= leaf_pos) goto err;
    return leaf_pos;

err:
    return -1;
}

///////////////////////////////////////////////////////////////////////////////
// COMMEN
///////////////////////////////////////////////////////////////////////////////
static inline int prefix_copy(char *filled_prefix_dst, uint8_t *prefix_dst_len, char *filled_prefix_src, uint8_t max_len) {
    if (CTDB_MAX_KEY_LEN < max_len) goto err;
    if (filled_prefix_dst == strncpy(filled_prefix_dst, filled_prefix_src, max_len)) {
        if (NULL != prefix_dst_len) *prefix_dst_len = strnlen(filled_prefix_dst, CTDB_MAX_KEY_LEN);
        return CTDB_OK;
    }

err:
    return CTDB_ERR;
}

static int item_cmp(const void *a, const void *b) {
    const struct ctdb_node_item *i = a, *j = b;
    return i->sub_prefix_char - j->sub_prefix_char;
}

static off_t find_node_from_file(int fd, off_t trav_pos, char *prefix, uint8_t prefix_len, uint8_t prefix_pos, uint8_t is_fuzzy, uint8_t *matched_prefix_len) {
    if(prefix_len == prefix_pos) return trav_pos;  //no need to match
    if(prefix_len > prefix_pos) {
        struct ctdb_node trav = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
        if (CTDB_OK != load_node(fd, trav_pos, &trav)) goto err;

        if (NULL != matched_prefix_len){
            *matched_prefix_len = prefix_pos;
        }
        
        //across the same prefix
        uint8_t key_prefix_pos = prefix_pos;
        uint8_t trav_prefix_pos = 0;
        while (trav_prefix_pos < trav.prefix_len && 
                key_prefix_pos < prefix_len && 
                prefix[key_prefix_pos] == trav.prefix[trav_prefix_pos]) {
            ++key_prefix_pos;
            ++trav_prefix_pos;
        };
        
        if (trav_prefix_pos == trav.prefix_len) {
            if(key_prefix_pos == prefix_len) {
                return trav_pos;  //the trav_node prefix must match completely
            }

            //traverse to the next node of the tree
            char prefix_char = prefix[key_prefix_pos];
            struct ctdb_node_item key_item = {.sub_prefix_char = prefix_char, .sub_node_pos = 0};
            struct ctdb_node_item *item = (struct ctdb_node_item *)bsearch(&key_item, trav.items, trav.items_count, sizeof(key_item), item_cmp);
            if (NULL == item) goto err;  //the item not found in child nodes, stop searching
            return find_node_from_file(fd, item->sub_node_pos, prefix, prefix_len, key_prefix_pos, is_fuzzy, matched_prefix_len);
        }
        //fuzzy matching
        if(is_fuzzy) {
            return trav_pos;
        }
        goto err;  //the current node's prefix does not match the key, stop searching
    }

err:
    return -1;
}

static int put_node_into_items(struct ctdb_node *father_node, char sub_prefix_char, off_t sub_node_pos) {
    if (0 >= sub_node_pos) goto err;
    struct ctdb_node_item new_item = {.sub_prefix_char = sub_prefix_char, .sub_node_pos = sub_node_pos};
    struct ctdb_node_item *exists_item = (struct ctdb_node_item *)bsearch(&new_item, father_node->items, father_node->items_count, sizeof(new_item), item_cmp);
    if (exists_item != NULL) {
        exists_item->sub_node_pos = sub_node_pos;
    } else {
        if (father_node->items_count + 1 >= CTDB_MAX_CHAR_RANGE) goto err;
        father_node->items[father_node->items_count++] = new_item;
        qsort(father_node->items, father_node->items_count, sizeof(struct ctdb_node_item), item_cmp);
    }
    return CTDB_OK;

err:
    return CTDB_ERR;
}

static off_t append_node_to_file(int fd, struct ctdb_node *trav, char *prefix, uint8_t prefix_len, uint8_t prefix_pos, off_t leaf_pos) {
    while (prefix_len > prefix_pos) { //this is not a loop, just for the 'break'
        char prefix_char = prefix[prefix_pos];
        struct ctdb_node_item key_item = {.sub_prefix_char = prefix_char, .sub_node_pos = 0};
        struct ctdb_node_item *item = (struct ctdb_node_item *)bsearch(&key_item, trav->items, trav->items_count, sizeof(key_item), item_cmp);
        if (NULL == item) break;  //the item not found in child nodes, stop searching

        //load node from the file
        struct ctdb_node sub_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
        if (CTDB_OK != load_node(fd, item->sub_node_pos, &sub_node)) goto err;

        //across the same prefix
        uint8_t key_prefix_pos = prefix_pos;
        uint8_t sub_node_prefix_pos = 0;
        while (sub_node_prefix_pos < sub_node.prefix_len && key_prefix_pos < prefix_len && 
                prefix[key_prefix_pos] == sub_node.prefix[sub_node_prefix_pos]) {
            ++key_prefix_pos;
            ++sub_node_prefix_pos;
        };

        if (sub_node_prefix_pos == sub_node.prefix_len) {
            //continue to traverse to the next node of the tree
            off_t new_node_pos = append_node_to_file(fd, &sub_node, prefix, prefix_len, key_prefix_pos, leaf_pos);
            if (CTDB_OK != put_node_into_items(trav, sub_node.prefix[0], new_node_pos)) goto err;
            return dump_node(fd, trav);  //append the node to the end of file

        } else {
            char old_remained[CTDB_MAX_KEY_LEN + 1] = {[0 ... CTDB_MAX_KEY_LEN] = 0};  //the old prefix does not include duplicate parts
            if (CTDB_OK != prefix_copy(old_remained, NULL, sub_node.prefix + sub_node_prefix_pos, CTDB_MAX_KEY_LEN)) goto err;
            char new_remained[CTDB_MAX_KEY_LEN + 1] = {[0 ... CTDB_MAX_KEY_LEN] = 0};  //the new prefix does not include duplicate parts
            if (CTDB_OK != prefix_copy(new_remained, NULL, prefix + key_prefix_pos, CTDB_MAX_KEY_LEN)) goto err;
            if (key_prefix_pos == prefix_len) {
                //the old prefix is longer and the new node should be inserted before the old node
                if (CTDB_OK != prefix_copy(sub_node.prefix, &sub_node.prefix_len, old_remained, CTDB_MAX_KEY_LEN)) goto err;

                //the old node as a child of the new node
                struct ctdb_node new_node = {.prefix_len = 0, .leaf_pos = leaf_pos, .items_count = 0};
                if (CTDB_OK != prefix_copy(new_node.prefix, &new_node.prefix_len, prefix + prefix_pos, CTDB_MAX_KEY_LEN)) goto err;
                if (CTDB_OK != put_node_into_items(&new_node, sub_node.prefix[0], dump_node(fd, &sub_node))) goto err;

                //the new node as a child of the trav node
                if (CTDB_OK != put_node_into_items(trav, new_node.prefix[0], dump_node(fd, &new_node))) goto err;
                return dump_node(fd, trav);  //append the node to the end of file

            } else {
                //the new prefix and the old prefix are not duplicate, split a common node to accommodate both
                struct ctdb_node common_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
                if (CTDB_OK != prefix_copy(common_node.prefix, &common_node.prefix_len, sub_node.prefix, sub_node_prefix_pos)) goto err;

                //the old node as a child of the common node
                if (CTDB_OK != prefix_copy(sub_node.prefix, &sub_node.prefix_len, old_remained, CTDB_MAX_KEY_LEN)) goto err;
                if (CTDB_OK != put_node_into_items(&common_node, sub_node.prefix[0], dump_node(fd, &sub_node))) goto err;

                //the new node as a child of the common node
                struct ctdb_node new_node = {.prefix_len = 0, .leaf_pos = leaf_pos, .items_count = 0};
                if (CTDB_OK != prefix_copy(new_node.prefix, &new_node.prefix_len, new_remained, CTDB_MAX_KEY_LEN)) goto err;
                if (CTDB_OK != put_node_into_items(&common_node, new_node.prefix[0], dump_node(fd, &new_node))) goto err;

                //the common node as a child of the trav node
                if (CTDB_OK != put_node_into_items(trav, common_node.prefix[0], dump_node(fd, &common_node))) goto err;
                return dump_node(fd, trav);  //append the node to the end of file
            }
        }
    }  //end:while
    
    if (prefix_len > prefix_pos) {
        //initialize the new node, or the new prefix is longer than the old prefix
        struct ctdb_node new_node = {.prefix_len = 0, .leaf_pos = leaf_pos, .items_count = 0};
        if (CTDB_OK != prefix_copy(new_node.prefix, &new_node.prefix_len, prefix + prefix_pos, CTDB_MAX_KEY_LEN)) goto err;
        if (CTDB_OK != put_node_into_items(trav, new_node.prefix[0], dump_node(fd, &new_node))) goto err;
        return dump_node(fd, trav);  //append the node to the end of file
        
    } else {
        //duplicate prefix, replace (written datas are never changed)
        trav->leaf_pos = leaf_pos;
        return dump_node(fd, trav);  //append the node to the end of file
    }

err:
    return -1;
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
        fd = open(path, O_RDWR | O_CREAT, 0666);
        if (0 > fd) goto err;
        if (SERIALIZER_OK != dump_header(fd)) goto err;
        if (SERIALIZER_OK != dump_footer(fd, &(struct ctdb_footer){ .tran_count=0, .del_count=0, .root_pos=0 })) goto err;
        if (-1 == fsync(db->fd)) goto err;
    } else {
        fd = open(path, O_RDWR);
        if (0 > fd) goto err;
        if (SERIALIZER_OK != check_header(fd)) goto err;
    }
    db->fd = fd;
    return db;

err:
    if (NULL != db) free(db);
    if (0 <= fd) close(fd);
    return NULL;
}

void ctdb_close(struct ctdb *db) {
    if (NULL == db) return;
    if (0 <= db->fd) close(db->fd);
    free(db);
}

struct ctdb_transaction *ctdb_transaction_begin(struct ctdb *db) {
    struct ctdb_transaction *trans = calloc(1, sizeof(*trans));
    if (NULL != trans) {
        if(CTDB_OK != load_footer(db->fd, &(trans->footer))){  //try to find the last transaction
            goto err;
        }
        trans->is_isvalid = 1;
        trans->db = db;
        return trans;
    }

err:
    if(NULL != trans){
        free(trans);
    }
    return NULL;
}

struct ctdb_leaf ctdb_get(struct ctdb_transaction *trans, char *key, uint8_t key_len) {
    struct ctdb_leaf leaf = { .value_len = 0, .value_pos = -1 };

    if (NULL == trans || 1 != trans->is_isvalid) goto err;  //verify that the transaction has not been committed or rolled back
    if (0 >= trans->footer.root_pos) goto err;
    if (0 >= key_len || CTDB_MAX_KEY_LEN < key_len || NULL == key) goto err;

    //search the prefix nodes related to key from the file
    char filled_prefix_key[CTDB_MAX_KEY_LEN + 1] = {[0 ... CTDB_MAX_KEY_LEN] = 0};
    if (filled_prefix_key != strncpy(filled_prefix_key, key, key_len)) goto err;
    off_t sub_node_pos = find_node_from_file(trans->db->fd, trans->footer.root_pos, filled_prefix_key, key_len, 0, 0, NULL);  //not fuzzy match
    if (0 >= sub_node_pos) goto err;  //no data found
    
    //load node from the file
    struct ctdb_node sub_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
    if (CTDB_OK != load_node(trans->db->fd, sub_node_pos, &sub_node)) goto err;
    if (0 >= sub_node.leaf_pos) goto err;

    if (CTDB_OK != load_leaf(trans->db->fd, sub_node.leaf_pos, &leaf)) goto err;
    if (0 == leaf.value_len) goto err;  //the data has been deleted
    return leaf;

err:
    leaf = (struct ctdb_leaf){ .value_len = 0, .value_pos = -1 };
    return leaf;
}

int ctdb_put(struct ctdb_transaction *trans, char *key, uint8_t key_len, char *value, uint32_t value_len) {
    if (NULL == trans || 1 != trans->is_isvalid) goto err;  //verify that the transaction has not been committed or rolled back
    if (0 >= key_len || CTDB_MAX_KEY_LEN < key_len || NULL == key) goto err;
    if (CTDB_MAX_VALUE_LEN < value_len || NULL == value) goto err;  //if value_len is 0, that means delete (whether it exists or not)

    struct ctdb_node root = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
    if (0 < trans->footer.root_pos) {
        if (CTDB_OK != load_node(trans->db->fd, trans->footer.root_pos, &root)) goto err;
    }

    //append the value and leaf node to the file
    off_t value_pos = append_to_end(trans->db->fd, value, value_len);
    if (0 >= value_pos) goto err;
    struct ctdb_leaf new_leaf = {.value_len = value_len, .value_pos = value_pos};
    off_t new_leaf_pos = dump_leaf(trans->db->fd, &new_leaf);
    if (0 >= new_leaf_pos) goto err;

    //update the prefix nodes (append only)
    char filled_prefix_key[CTDB_MAX_KEY_LEN + 1] = {[0 ... CTDB_MAX_KEY_LEN] = 0};
    if (filled_prefix_key != strncpy(filled_prefix_key, key, key_len)) goto err;
    off_t new_root_pos = append_node_to_file(trans->db->fd, &root, filled_prefix_key, key_len, 0, new_leaf_pos);
    if (0 >= new_root_pos) goto err;
    trans->footer.root_pos = new_root_pos;

    //cumulative the operation count (the transaction is not written to the file until committed)
    trans->footer.tran_count += 1;
    if (0 >= value_len || NULL == value)
        trans->footer.del_count += 1;
    return CTDB_OK;

err:
    return CTDB_ERR;
}

int ctdb_del(struct ctdb_transaction *trans, char *key, uint8_t key_len) {
    return ctdb_put(trans, key, key_len, "", 0);
}

int ctdb_transaction_commit(struct ctdb_transaction *trans) {
    if (NULL == trans || 1 != trans->is_isvalid) goto err;  //verify that the transaction has not been committed or rolled back
    trans->is_isvalid = 0;  //the transaction that have been used (commit, rollback) cannot be used any more

    struct ctdb *db = trans->db;
    //save the 'transaction flag', which means that the transaction was committed successfully
    if (CTDB_OK != dump_footer(db->fd, &(trans->footer))) goto err;
    if (-1 == fsync(db->fd)) goto err;
    return CTDB_OK;

err:
    return CTDB_ERR;
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
static int iterator_travel(int fd, struct ctdb_node *trav, char *key, uint8_t key_len, ctdb_traversal *traversal) {
    if ((trav->prefix_len + key_len) < CTDB_MAX_KEY_LEN) {
        char prefix_key[CTDB_MAX_KEY_LEN + 1] = {[0 ... CTDB_MAX_KEY_LEN] = 0};
        if (0 > sprintf(prefix_key, "%.*s%.*s", key_len, key, trav->prefix_len, trav->prefix)) goto over;
        uint8_t prefix_key_len = key_len + trav->prefix_len;

        if (0 < trav->leaf_pos) {
            struct ctdb_leaf leaf = {.value_len = 0, .value_pos = -1};
            if (CTDB_OK != load_leaf(fd, trav->leaf_pos, &leaf)) goto over;
            if (0 < prefix_key_len && 0 < leaf.value_len) {                             //the data has not been deleted
                if (CTDB_OK != traversal(fd, prefix_key, prefix_key_len, leaf)) goto over;  //the traversal operation has been cancelled
            }
        }

        int items_index = 0;
        for (; items_index < trav->items_count; items_index++) {
            off_t sub_node_pos = trav->items[items_index].sub_node_pos;
            struct ctdb_node sub_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
            if (CTDB_OK != load_node(fd, sub_node_pos, &sub_node)) goto over;
            if (CTDB_OK != iterator_travel(fd, &sub_node, prefix_key, prefix_key_len, traversal)){
                goto over; //something wrong, or the traversal operation has been cancelled
            }
        }
    }
    return CTDB_OK;

over:
    return CTDB_ERR;
}

int ctdb_iterator_travel(struct ctdb_transaction *trans, char *key, uint8_t key_len, ctdb_traversal *traversal) {
    if (NULL == trans || 1 != trans->is_isvalid) goto err;  //verify that the transaction has not been committed or rolled back
    if(CTDB_MAX_KEY_LEN < key_len) goto err;

    //search the prefix nodes related to key from the file
    char filled_prefix_key[CTDB_MAX_KEY_LEN + 1] = {[0 ... CTDB_MAX_KEY_LEN] = 0};
    if (filled_prefix_key != strncpy(filled_prefix_key, key, key_len)) goto err;
    uint8_t matched_prefix_len = 0;
    off_t sub_node_pos = find_node_from_file(trans->db->fd, trans->footer.root_pos, filled_prefix_key, key_len, 0, 1, &matched_prefix_len);  //fuzzy match
    if (0 >= sub_node_pos) goto err;  //no data found

    //load the starting node of traversal from the file
    struct ctdb_node sub_node = {.prefix_len = 0, .leaf_pos = 0, .items_count = 0};
    if (CTDB_OK == load_node(trans->db->fd, sub_node_pos, &sub_node)){
        return iterator_travel(trans->db->fd, &sub_node, filled_prefix_key, matched_prefix_len, traversal);
    }

err:
    return CTDB_ERR;
}
