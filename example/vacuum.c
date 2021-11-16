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
        //char *key = random_str_shortly(key_len);
        //char *key = random_str_num(key_len);

        assert(CTDB_OK == ctdb_put(trans, key, key_len, key, key_len));
        free(key);
    }
    ctdb_transaction_commit(trans);
    printf("stress: %d pieces of data in 1 transaction, time consuming:%ldms del_count:%lu tran_count:%lu\n", count, getCurrentTime() - start, trans->footer.del_count, trans->footer.tran_count);
    ctdb_transaction_free(&trans);

    start = getCurrentTime();
    assert(NULL != (trans = ctdb_transaction_begin(db)));


    struct ctdb *new_db = ctdb_open("test_tmp.db");
    assert(NULL != new_db);
    assert(CTDB_OK == ctdb_vacuum(trans, new_db));
    printf("vacuum: time consuming:%ldms tran_count:%lu\n", getCurrentTime() - start, trans->footer.tran_count);
    ctdb_transaction_free(&trans);
    ctdb_close(&new_db);
    ctdb_close(&db);
}

#define LAMBDA(return_type, function_body) \
    ({ \
        return_type __fn__ function_body \
        __fn__; \
    })

//check
void test_iter() {
    char *path = "./test_tmp.db";
    struct ctdb *db = ctdb_open(path);
    assert(NULL != db);
    
    struct ctdb_transaction *trans = ctdb_transaction_begin(db);
    assert(NULL != trans);
    int g_iter_count = 0;
    int res = CTDB_FOREACH(trans, "", 0, 
                (int fd, char *key, uint8_t key_len, struct ctdb_leaf leaf){
                    g_iter_count += 1;
                    assert(0 < leaf.value_len);
                    assert(0 < leaf.value_pos);
                    char *value = read_value_from_file(db->fd, leaf.value_len, leaf.value_pos);
                    //printf("key:%.*s value:%.*s\n", key_len, key, leaf.value_len, value);
                    assert(key_len == leaf.value_len && 0 == strncmp(key, value, key_len));
                    free(value);
                    return CTDB_OK; //continue 
                }
            );
    if(CTDB_OK == res){
        assert(trans->footer.tran_count == g_iter_count);
        printf("iterator new_db sucess, tran_count:%lu iter_count:%d\n", trans->footer.tran_count, g_iter_count);
    }else{
        printf("iterator new_db failed!!! : %d\n", g_iter_count);
    }
    ctdb_transaction_free(&trans);
    ctdb_close(&db);
}

int main(){
    srand(time(NULL));

    stress_put_testing_single_transaction(32, 2500);
    test_iter();
    
    printf("over\n");
    return 0;
}
