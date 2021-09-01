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

///////////////////////////////////////////////////////////////////////////////
// IO
///////////////////////////////////////////////////////////////////////////////
#define SERIALIZER_IO_READ_BYTES(fd,res,len) \
    ({ \
        int readn = read((fd), (res), (len)); \
        readn == (len) ? SERIALIZER_OK : SERIALIZER_ERR; \
    })

#define SERIALIZER_IO_WRITE_BYTES(fd,val,len) \
    ({ \
        int writen = write((fd), (val), (len)); \
        writen == (len) ? SERIALIZER_OK : SERIALIZER_ERR; \
    })

#define SERIALIZER_IO_READ_NUM(fd,res,type) \
    ({ \
        int opr = SERIALIZER_ERR; \
        type val = 0; \
        if(0 <= (fd)){ \
            opr = SERIALIZER_IO_READ_BYTES((fd), &val, sizeof(type)); \
            if(SERIALIZER_OK == opr){ \
                (res) = swapping_##type(val); \
            } \
        } \
        opr; \
    })

#define SERIALIZER_IO_WRITE_NUM(fd,val,type) \
    ({ \
        int opr = SERIALIZER_ERR; \
        type v = swapping_##type((type)(val)); \
        if(0 <= (fd)){ \
            opr = SERIALIZER_IO_WRITE_BYTES((fd), &v, sizeof(type)); \
        } \
        opr; \
    })

#define SERIALIZER_IO_READ_STR(fd,res,len) \
    ({ \
        int opr = SERIALIZER_ERR; \
        if(0 <= (fd) && NULL != (res)){ \
            (res)[0] = '\0'; \
            opr = SERIALIZER_IO_READ_BYTES((fd), (res), (len)); \
        } \
        opr; \
    })

#define SERIALIZER_IO_READ_NEW_STR(fd,res,len) \
    ({ \
        int opr = SERIALIZER_ERR; \
        (res) = calloc(1, (len) + 1); \
        if(0 <= (fd) && NULL != (res)){ \
            opr = SERIALIZER_IO_READ_BYTES((fd), (res), (len)); \
        } \
        opr; \
    })

#define SERIALIZER_IO_WRITE_STR(fd,val,len) \
    ({ \
        int opr = SERIALIZER_ERR; \
        if(0 <= (fd) && NULL != (val)){ \
            opr = SERIALIZER_IO_WRITE_BYTES((fd), (val), (len)); \
        } \
        opr; \
    })

///////////////////////////////////////////////////////////////////////////////
// BUFFER
///////////////////////////////////////////////////////////////////////////////
#define SERIALIZER_BUF_READ_BYTES(buf,end,res,len) \
    ({ \
        char *start = (buf); \
        if(NULL != (buf) && 0 < (len) && (end + 1 - buf) >= (len)){ \
            memcpy((res), (buf), (len)); \
            (buf) += (len); \
        } \
        (len) == ((buf) - start) ? SERIALIZER_OK : SERIALIZER_ERR; \
    })

#define SERIALIZER_BUF_WRITE_BYTES(buf,end,val,len) \
    ({ \
        char *start = (buf); \
        if(NULL != (buf) && 0 < (len) && (end + 1 - buf) >= (len)){ \
            memcpy((buf), (val), (len)); \
            (buf) += (len); \
        } \
        (len) == ((buf) - start) ? SERIALIZER_OK : SERIALIZER_ERR; \
    })

#define SERIALIZER_BUF_SKIP_BYTES(buf,end,len) \
    ({ \
        char *start = (buf); \
        if(NULL != (buf) && 0 < (len) && (end + 1 - buf) >= (len)){ \
            (buf) += (len); \
        } \
        (len) == ((buf) - start) ? SERIALIZER_OK : SERIALIZER_ERR; \
    })

#define SERIALIZER_BUF_READ_NUM(buf,end,res,type) \
    ({ \
        type val_tmp; \
        int opr = SERIALIZER_BUF_READ_BYTES((buf), (end), &val_tmp, sizeof(type)); \
        if(SERIALIZER_OK == opr){ \
            (res) = swapping_##type(val_tmp); \
        } \
        opr; \
    })

#define SERIALIZER_BUF_WRITE_NUM(buf,end,val,type) \
    ({ \
        type val_tmp = swapping_##type((type)(val)); \
        SERIALIZER_BUF_WRITE_BYTES((buf), (end), &val_tmp, sizeof(type)); \
    })

#define SERIALIZER_BUF_READ_STR(buf,end,res,len) \
    ({ \
        SERIALIZER_BUF_READ_BYTES((buf), (end), (res), (len)); \
    })

#define SERIALIZER_BUF_READ_NEW_STR(buf,end,res,len) \
    ({ \
        (res) = calloc(1, (len) + 1); \
        SERIALIZER_BUF_READ_BYTES((buf), (end), (res), (len)); \
    })

#define SERIALIZER_BUF_WRITE_STR(buf,end,val,len) \
    ({ \
        SERIALIZER_BUF_WRITE_BYTES((buf), (end), (val), (len)); \
    })

#ifdef __cplusplus
}
#endif
#endif
