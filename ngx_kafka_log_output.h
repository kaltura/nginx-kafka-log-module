/*
 * Copyright (C) 2017 Paulo Pacheco
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#ifndef __NGX_KAFKA_LOG_OUTPUT_H__
#define __NGX_KAFKA_LOG_OUTPUT_H__

#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_kafka_log_kafka.h"

#define NGX_KAFKA_LOG_FILE_OUT_LEN (sizeof("file:") - 1)
#define NGX_KAFKA_LOG_HAS_FILE_PREFIX(str)                   \
    (ngx_strncmp(str->data,                                 \
                 "file:",                                   \
                 NGX_KAFKA_LOG_FILE_OUT_LEN) ==  0 )

#define NGX_KAFKA_LOG_KAFKA_OUT_LEN (sizeof("kafka:") - 1)
#define NGX_KAFKA_LOG_HAS_KAFKA_PREFIX(str)                  \
    (ngx_strncmp(str->data,                                 \
                 "kafka:",                                  \
                 NGX_KAFKA_LOG_KAFKA_OUT_LEN) ==  0 )

typedef enum {
    NGX_KAFKA_LOG_SINK_FILE = 0,
    NGX_KAFKA_LOG_SINK_KAFKA = 1
} ngx_kafka_log_sink_e;

typedef struct {
    ngx_str_t                                 location;
    ngx_kafka_log_sink_e                       type;
    ngx_http_complex_value_t                  cv;
    ngx_open_file_t                           *file;
#if (NGX_HAVE_LIBRDKAFKA)
    ngx_kafka_log_kafka_conf_t                 kafka;
    ngx_queue_t queue;
#endif
} ngx_kafka_log_output_location_t;

ngx_int_t ngx_kafka_log_write_sink_file(
    ngx_log_t *log,
    ngx_open_file_t* file,
    ngx_str_t* txt);

#endif // __NGX_KAFKA_LOG_OUTPUT_H__

