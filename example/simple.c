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

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include <sys/time.h>
#include <time.h>

#include "serializer.h"
#include "ctdb.h"

///////////////////////////////////////////////////////////////////////////////
// UTILS
///////////////////////////////////////////////////////////////////////////////
static int random_range(int min, int max) __attribute__((unused));
static int random_range(int min, int max){
    return rand() % (max - min + 1) + min;
}

static int64_t getCurrentTime() __attribute__((unused));
static int64_t getCurrentTime() {
   struct timeval tv;
   gettimeofday(&tv,NULL);
   return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static char *random_str(int len) __attribute__((unused));
static char *random_str(int len) {
    int i;
    char *alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    char *string = malloc(len + 1);
    if(NULL == string) return NULL;
    for (i = 0; i < len; i++) {
        string[i] = alphabet[rand() % 62];
    }
    string[i] = 0;
    return string;
}

static char *random_str_shortly(int len) __attribute__((unused));
static char *random_str_shortly(int len) {
    int i;
    char *alphabet = "abcdefghijklmnopqrstuvwxyz0123456789._";
    char *string = malloc(len + 1);
    if(NULL == string) return NULL;
    for (i = 0; i < len; i++) {
        string[i] = alphabet[rand() % 38];
    }
    string[i] = 0;
    return string;
}

static char *random_str_num(int len) __attribute__((unused));
static char *random_str_num(int len) {
    int i;
    char *alphabet = "0123456789";
    char *string = malloc(len + 1);
    if(NULL == string) return NULL;
    for (i = 0; i < len; i++) {
        string[i] = alphabet[rand() % 10];
    }
    string[i] = 0;
    return string;
}

///////////////////////////////////////////////////////////////////////////////
// TESTING
///////////////////////////////////////////////////////////////////////////////
void simple_test() __attribute__((unused));
void simple_test() {
    char *path = "./test.db";
    struct ctdb *db = ctdb_open(path);
    assert(NULL != db);

    struct ctdb_transaction *trans = ctdb_transaction_begin(db);
    assert(NULL != trans);
    assert(CTDB_OK == ctdb_put(trans, "apple", 5, "apple_value", 11));
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);
    
    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_put(trans, "app", 3, "app_value", 9));
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);

    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_put(trans, "application", 11, "application_value", 17));
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);

    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_put(trans, "xxx", 3, "hahahahaha", 10));
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);

    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_put(trans, "app", 3, "app_6666666669", 14));
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);
    ctdb_close(db);

    printf("-----------------------\n");

    db = ctdb_open(path);
    assert(NULL != db);

    printf("after put: del_count:%llu tran_count:%llu\n", db->footer.del_count, db->footer.tran_count);
    struct ctdb_leaf *leaf = ctdb_get(db, "app", 3);
    assert(NULL != leaf);
    printf("app: %.*s\n", leaf->value_len, leaf->value);
    ctdb_leaf_free(leaf);

    leaf = ctdb_get(db, "apple", 5);
    assert(NULL != leaf);
    printf("apple: %.*s\n", leaf->value_len, leaf->value);
    ctdb_leaf_free(leaf);
    
    leaf = ctdb_get(db, "application", 11);
    assert(NULL != leaf);
    printf("application: %.*s\n", leaf->value_len, leaf->value);
    ctdb_leaf_free(leaf);

    leaf = ctdb_get(db, "xxx", 3);
    assert(NULL != leaf);
    printf("xxx: %.*s\n", leaf->value_len, leaf->value);
    ctdb_leaf_free(leaf);

    printf("-----------------------\n");

    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_del(trans, "app", 3));
    assert(CTDB_OK == ctdb_del(trans, "apple", 5));
    assert(CTDB_OK == ctdb_del(trans, "application", 11));
    assert(CTDB_OK == ctdb_del(trans, "xxx", 3));
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);
    
    leaf = ctdb_get(db, "xxx", 3);
    assert(NULL == leaf);
    printf("after del: del_count:%llu tran_count:%llu\n", db->footer.del_count, db->footer.tran_count);

    ctdb_close(db);
}

void stress_put_testing_single_transaction(int key_len, int count) __attribute__((unused));
void stress_put_testing_single_transaction(int key_len, int count) {
    char *path = "./test.db";
    struct ctdb *db = ctdb_open(path);
    assert(NULL != db);
    assert(key_len <= CTDB_MAX_KEY_LEN);

    struct ctdb_transaction *trans = ctdb_transaction_begin(db);
    assert(NULL != trans);

    int64_t start = getCurrentTime();
    int i = 0;
    for(; i < count; i++){
        char *key = random_str(key_len);
        assert(CTDB_OK == ctdb_put(trans, key, key_len, key, key_len));
        free(key);
    }
    ctdb_transaction_commit(trans);
    printf("stress: %d pieces of data in 1 transaction, time consuming:%lldms del_count:%llu tran_count:%llu\n", count, getCurrentTime() - start, db->footer.del_count, db->footer.tran_count);
    ctdb_transaction_free(trans);
    ctdb_close(db);
}

void stress_put_testing_multiple_transactions(int count) __attribute__((unused));
void stress_put_testing_multiple_transactions(int count) {
    int key_len = random_range(1, CTDB_MAX_KEY_LEN);

    char *path = "./test.db";
    struct ctdb *db = ctdb_open(path);
    assert(NULL != db);
    assert(key_len <= CTDB_MAX_KEY_LEN);

    int64_t start = getCurrentTime();
    int i = 0;
    for(; i < count; i++){
        char *key = random_str(key_len);
        //char *key = random_str_shortly(key_len);
        //char *key = random_str_num(key_len);

        struct ctdb_transaction *trans = ctdb_transaction_begin(db);
        assert(NULL != trans);
        assert(CTDB_OK == ctdb_put(trans, key, key_len, key, key_len));
        assert(NULL != trans);
        assert(CTDB_OK == ctdb_transaction_commit(trans));
        ctdb_transaction_free(trans);
        free(key);
    }
    printf("stress: %d pieces of data in %d transactions, time consuming:%lldms del_count:%llu tran_count:%llu\n", count, count, getCurrentTime() - start, db->footer.del_count, db->footer.tran_count);
    ctdb_close(db);
}

void stress_get_testing(int count) __attribute__((unused));
void stress_get_testing(int count) {
    int key_len = random_range(1, 32);

    char *path = "./test.db";
    struct ctdb *db = ctdb_open(path);
    assert(NULL != db);
    assert(key_len <= CTDB_MAX_KEY_LEN);

    char *test_key = "t5gc8oko0a1uyrfb6xbf6bwsf877y44q";
    int test_key_len = 32;

    struct ctdb_transaction *trans = ctdb_transaction_begin(db);
    assert(NULL != trans);
    assert(CTDB_OK == ctdb_put(trans, test_key, test_key_len, test_key, test_key_len));
    assert(NULL != trans);
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);
    
    assert(NULL != (trans = ctdb_transaction_begin(db)));
    int i = 1;
    for(; i < count; i++){
        char *key = random_str(key_len);
        assert(CTDB_OK == ctdb_put(trans, key, key_len, key, key_len));
        free(key);
    }
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);

    int64_t start = getCurrentTime();
    i = 0;
    for(; i < count; i++){
        struct ctdb_leaf *leaf = ctdb_get(db, test_key, test_key_len);
        assert(NULL != leaf);
        assert(test_key_len == leaf->value_len && 0 == strncmp(test_key, leaf->value, test_key_len));
        ctdb_leaf_free(leaf);
    }
    printf("stress: %d pieces of data read operation, time consuming:%lldms del_count:%llu tran_count:%llu\n", count, getCurrentTime() - start, db->footer.del_count, db->footer.tran_count);
    ctdb_close(db);
}

int main(){
    srand(time(NULL));

    simple_test();
    stress_put_testing_single_transaction(5, 10000);
    stress_put_testing_multiple_transactions(5000);
    stress_get_testing(50000);
    
    printf("over\n");
    return 0;
}
