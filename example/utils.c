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

int random_range(int min, int max){
    return rand() % (max - min + 1) + min;
}

int64_t getCurrentTime() {
   struct timeval tv;
   gettimeofday(&tv,NULL);
   return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

char *random_str(int len) {
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

char *random_str_shortly(int len) {
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

char *random_str_num(int len) {
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

char *read_value_from_file(int fd, uint32_t value_len, off_t value_pos) {
    char *value = calloc(1, value_len);
    if (NULL == value) goto err;
    if (-1 == lseek(fd, value_pos, SEEK_SET)) goto err;
    if (value_len != read(fd, value, value_len)) goto err;
    return value;

err:
    if (NULL != value) free(value);
    return NULL;
}
