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
static int random_range() __attribute__((unused));
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
int g_iter_count = 0;
#if defined(__APPLE__)
    int traversal(char *key, uint8_t key_len, struct ctdb_leaf *leaf){
        g_iter_count += 1;
        printf("key:%.*s value:%.*s\n", key_len, key, leaf->value_len, leaf->value);
        assert(key_len == leaf->value_len && 0 == strncmp(key, leaf->value, key_len));
        return CTDB_OK; //continue
    }
#endif
void test_iter(int count, char *prefix, uint8_t prefix_len) {
    int key_len = 32;
    char *path = "./test.db";
    struct ctdb *db = ctdb_open(path);
    assert(NULL != db);
    
    struct ctdb_transaction *trans = ctdb_transaction_begin(db);
    assert(NULL != trans);

    assert(CTDB_OK == ctdb_put(trans, "apple", 5, "apple", 5));
    assert(CTDB_OK == ctdb_put(trans, "app", 3, "app", 3));
    assert(CTDB_OK == ctdb_put(trans, "application", 11, "application", 11));

    int i = 0;
    for(; i < count; i++){
        char *key = random_str_shortly(key_len);
        assert(CTDB_OK == ctdb_put(trans, key, key_len, key, key_len));
        free(key);
    }

    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(trans);
   
#if defined(__APPLE__)
    if(CTDB_OK == ctdb_iterator_travel(db, prefix, prefix_len, traversal)){
        printf("iterator sucess, prefix:'%s' count:%ld iter_count:%d\n", prefix, db->footer.tran_count, g_iter_count);
    }
#else
    int traversal(char *key, uint8_t key_len, struct ctdb_leaf *leaf){
        g_iter_count += 1;
        printf("key:%.*s value:%.*s\n", key_len, key, leaf->value_len, leaf->value);
        assert(key_len == leaf->value_len && 0 == strncmp(key, leaf->value, key_len));
        return CTDB_OK; //continue
    }
    if(CTDB_OK == ctdb_iterator_travel(db, prefix, prefix_len, traversal)){
        printf("iterator sucess, prefix:'%s' count:%ld iter_count:%d\n", prefix, db->footer.tran_count, g_iter_count);
    }
#endif
    ctdb_close(db);
}

int main(){
    srand(time(NULL));
    
    test_iter(100, "", 0);  //traverse all data
    g_iter_count = 0;
    test_iter(50, "ap", 2);  //traverse the specified data

    printf("over\n");
    return 0;
}
