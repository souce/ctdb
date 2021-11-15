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

#ifndef __SERIALIZER_H_
#define __SERIALIZER_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>

#define swap_8 /* do nothing */
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__ /* swap to little-endian */
    #if defined(__APPLE__)
        #include <libkern/OSByteOrder.h>
        #define swap_16 OSSwapInt16
        #define swap_32 OSSwapInt32
        #define swap_64 OSSwapInt64
    #else
        #include <byteswap.h>
        #define swap_16 bswap_16
        #define swap_32 bswap_32
        #define swap_64 bswap_64
    #endif
#elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__ /* already little-endian, do nothing */
    #define swap_16
    #define swap_32
    #define swap_64
#else
    #error unknow endian
#endif

#define DEFINE_NUM_SWAPPING(T) \
    static T swapping_##T(T value)__attribute__((unused)); \
    static T swapping_##T(T value){ \
        T res = 0; \
        switch ( sizeof(T) ) { \
            case 1: *(uint8_t*)&res = swap_8(*(uint8_t*)&value); break; \
            case 2: *(uint16_t*)&res = swap_16(*(uint16_t*)&value); break; \
            case 4: *(uint32_t*)&res = swap_32(*(uint32_t*)&value); break; \
            case 8: *(uint64_t*)&res = swap_64(*(uint64_t*)&value); break; \
            default: return 0; \
        } \
        return res; \
    }
DEFINE_NUM_SWAPPING(int8_t)
DEFINE_NUM_SWAPPING(uint8_t)
DEFINE_NUM_SWAPPING(int16_t)
DEFINE_NUM_SWAPPING(uint16_t)
DEFINE_NUM_SWAPPING(int32_t)
DEFINE_NUM_SWAPPING(uint32_t)
DEFINE_NUM_SWAPPING(int64_t)
DEFINE_NUM_SWAPPING(uint64_t)
DEFINE_NUM_SWAPPING(float)
DEFINE_NUM_SWAPPING(double)

#define SERIALIZER_OK 0
#define SERIALIZER_ERR -1

struct serializer{
    char *buf;
    size_t buf_len;
    off_t offset;
};

#define SERIALIZER_INIT(len) \
    ((struct serializer){ \
        .buf = (char[(len)]){}, \
        .buf_len = (len), \
        .offset = 0 \
    })

#define SERIALIZER_READ_BYTES(ser,res,len) \
    ({ \
        off_t start = (ser).offset; \
        if ((uintptr_t)NULL != (uintptr_t)((ser).buf) && \
            (uintptr_t)NULL != (uintptr_t)(res) && \
            0 < (len) && \
            (ser).offset + (len) <= (ser).buf_len){ \
                memcpy((res), (ser).buf + (ser).offset, (len)); \
                (ser).offset += (len); \
        } \
        (len) == (ser).offset - start ? SERIALIZER_OK : SERIALIZER_ERR; \
    })

#define SERIALIZER_WRITE_BYTES(ser,val,len) \
    ({ \
        off_t start = (ser).offset; \
        if ((uintptr_t)NULL != (uintptr_t)((ser).buf) && \
            (uintptr_t)NULL != (uintptr_t)(val) && \
            0 < (len) && \
            (ser).offset + (len) <= (ser).buf_len){ \
                memcpy((ser).buf + (ser).offset, (val), (len)); \
                (ser).offset += (len); \
        } \
        (len) == (ser).offset - start ? SERIALIZER_OK : SERIALIZER_ERR; \
    })

#define SERIALIZER_SKIP_BYTES(ser,len) \
    ({ \
        off_t start = (ser).offset; \
        if ((uintptr_t)NULL != (uintptr_t)((ser).buf) && \
            0 < (len) && \
            (ser).offset + (len) <= (ser).buf_len){ \
                (ser).offset += (len); \
        } \
        (len) == (ser).offset - start ? SERIALIZER_OK : SERIALIZER_ERR; \
    })

#define SERIALIZER_READ_NUM(ser,res,type) \
    ({ \
        type val_tmp; \
        int opr = SERIALIZER_READ_BYTES((ser), &val_tmp, sizeof(type)); \
        if(SERIALIZER_OK == opr){ \
            (res) = swapping_##type(val_tmp); \
        } \
        opr; \
    })

#define SERIALIZER_WRITE_NUM(ser,val,type) \
    ({ \
        type val_tmp = swapping_##type((type)(val)); \
        SERIALIZER_WRITE_BYTES((ser), &val_tmp, sizeof(type)); \
    })

#define SERIALIZER_READ_STR(ser,res,len) \
    ({ \
        SERIALIZER_READ_BYTES((ser), (res), (len)); \
    })

#define SERIALIZER_READ_NEW_STR(ser,res,len) \
    ({ \
        (res) = calloc(1, (len) + 1); \
        SERIALIZER_READ_BYTES((ser), (res), (len)); \
    })

#define SERIALIZER_WRITE_STR(ser,val,len) \
    ({ \
        SERIALIZER_WRITE_BYTES((ser), (val), (len)); \
    })

#ifdef __cplusplus
}
#endif
#endif
