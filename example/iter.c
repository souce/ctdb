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
#include "utils.h"

int g_iter_count = 0;
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
    ctdb_transaction_free(&trans);
    
    assert(NULL != (trans = ctdb_transaction_begin(db)));
    g_iter_count = 0;
    
    int traversal(int fd, char *key, uint8_t key_len, struct ctdb_leaf leaf){
        g_iter_count += 1;
        assert(0 < leaf.value_len);
        assert(0 < leaf.value_pos);
        char *value = read_value_from_file(fd, leaf.value_len, leaf.value_pos);
        printf("key:%.*s value:%.*s\n", key_len, key, leaf.value_len, value);
        assert(key_len == leaf.value_len && 0 == strncmp(key, value, key_len));
        free(value);
        return CTDB_OK; //continue
    }
    if(CTDB_OK == ctdb_iterator_travel(trans, prefix, prefix_len, traversal)){
        printf("iterator sucess, prefix:'%s' count:%lu iter_count:%d\n", prefix, trans->footer.tran_count, g_iter_count);
    }else{
        printf("iterator failed!!!\n");
    }

    ctdb_transaction_free(&trans);
    ctdb_close(&db);
}

int main(){
    srand(time(NULL));
    
    test_iter(100, "", 0);  //traverse all data
    test_iter(50, "ap", 2);  //traverse the specified data

    printf("over\n");
    return 0;
}
