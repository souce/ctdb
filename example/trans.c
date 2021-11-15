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

void transction_test()__attribute__((unused));
void transction_test() {
    char *path = "./test.db";
    struct ctdb *db = ctdb_open(path);
    assert(NULL != db);

    struct ctdb_transaction *trans = ctdb_transaction_begin(db);
    assert(NULL != trans);
    assert(CTDB_OK == ctdb_put(trans, "apple", 5, "apple_value", 11));
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(&trans);
    
    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_put(trans, "app", 3, "app_value", 9));
    assert(NULL != trans);
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(&trans);

    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_put(trans, "application", 11, "application_value", 17));
    assert(NULL != trans);
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(&trans);

    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_put(trans, "app", 3, "app_new_value", 13));
    assert(NULL != trans);
    ctdb_transaction_rollback(trans); //rollback！ "app" still "app_value"
    ctdb_transaction_free(&trans);
    ctdb_close(&db);

    printf("-----------------------\n");
    
    char *value = NULL;
    assert(NULL != (db = ctdb_open(path)));
    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(0 == trans->footer.del_count);
    assert(3 == trans->footer.tran_count);
    struct ctdb_leaf leaf = ctdb_get(trans, "app", 3);
    assert(0 < leaf.value_len);
    assert(0 < leaf.value_pos);
    assert(NULL != (value = read_value_from_file(db->fd, leaf.value_len, leaf.value_pos)));
    printf("app: %.*s\n", leaf.value_len, value);
    free(value);
    //printf("app valua is: '%.*s'\n", leaf.value_len, leaf.value);
    ctdb_transaction_free(&trans);
    ctdb_close(&db);
}

/*
    After the transaction test, open the file again
*/
void transction_test2() __attribute__((unused));
void transction_test2() {
    char *path = "./test.db";
    struct ctdb *db = ctdb_open(path);
    assert(NULL != db);

    struct ctdb_transaction *trans = ctdb_transaction_begin(db);
    assert(NULL != trans);
    assert(CTDB_OK == ctdb_put(trans, "app", 3, "app_new_value", 13));
    ctdb_transaction_commit(trans);
    ctdb_transaction_free(&trans);
    ctdb_close(&db);

    printf("-----------------------\n");
    
    char *value = NULL;
    assert(NULL != (db = ctdb_open(path)));
    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(0 == trans->footer.del_count);
    assert(4 == trans->footer.tran_count);
    struct ctdb_leaf leaf = ctdb_get(trans, "app", 3);
    assert(0 < leaf.value_len);
    assert(0 < leaf.value_pos);
    assert(NULL != (value = read_value_from_file(db->fd, leaf.value_len, leaf.value_pos)));
    printf("app: %.*s\n", leaf.value_len, value);
    free(value);
    ctdb_transaction_free(&trans);
    ctdb_close(&db);

    printf("-----------------------\n");

    db = ctdb_open(path);
    assert(NULL != (trans = ctdb_transaction_begin(db)));
    assert(CTDB_OK == ctdb_put(trans, "app", 3, "app_value", 13));
    assert(CTDB_OK == ctdb_transaction_commit(trans));
    ctdb_transaction_free(&trans);
    
    assert(NULL != (trans = ctdb_transaction_begin(db)));
    leaf = ctdb_get(trans, "app", 3);
    assert(0 < leaf.value_len);
    assert(0 < leaf.value_pos);
    assert(NULL != (value = read_value_from_file(db->fd, leaf.value_len, leaf.value_pos)));
    printf("app: %.*s\n", leaf.value_len, value);
    free(value);
    ctdb_transaction_free(&trans);
    ctdb_close(&db);
}

int main(){
    srand(time(NULL));
    
    transction_test();
    transction_test2();

    printf("over\n");
    return 0;
}
