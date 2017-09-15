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
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_log.h>

#include <ctype.h>
#include <assert.h>

#include "ngx_kafka_log_output.h"
#include "ngx_kafka_log_kafka.h"

// typedefs
typedef struct {
#if (NGX_HAVE_LIBRDKAFKA)
    ngx_kafka_log_main_kafka_conf_t kafka;
#endif
} ngx_http_kafka_log_main_conf_t;

typedef struct {
    ngx_array_t *locations;
} ngx_http_kafka_log_loc_conf_t;

// forward decls
static char *ngx_http_kafka_log_loc_output(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void *ngx_http_kafka_log_create_main_conf(ngx_conf_t *cf);
static ngx_int_t ngx_http_kafka_log_init_worker(ngx_cycle_t *cycle);
static void *ngx_http_kafka_log_create_loc_conf(ngx_conf_t *cf);
static ngx_int_t ngx_http_kafka_log_post_config(ngx_conf_t *cf);

// globals
static ngx_command_t ngx_http_kafka_log_commands[] = {
    { 
        ngx_string("kafka_log"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE23,
        ngx_http_kafka_log_loc_output,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },
#if (NGX_HAVE_LIBRDKAFKA)
    {
        ngx_string("kafka_log_kafka_client_id"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.client_id),
        NULL
    },
    {
        ngx_string("kafka_log_kafka_debug"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.debug),
        NULL
    },
    {
        ngx_string("kafka_log_kafka_brokers"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.brokers),
        NULL
    },
    {
        ngx_string("kafka_log_kafka_compression"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.compression),
        NULL
    },
    {
        ngx_string("kafka_log_kafka_partition"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.partition),
        NULL
    },
    {
        ngx_string("kafka_log_kafka_log_level"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.log_level),
        NULL
    },
    {
        ngx_string("kafka_log_kafka_max_retries"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.max_retries),
        NULL
    },
    {
        ngx_string("kafka_log_kafka_buffer_max_messages"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.buffer_max_messages),
        NULL
    },
    {
        ngx_string("kafka_log_kafka_backoff_ms"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_msec_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_log_main_conf_t, kafka.backoff_ms),
        NULL
    },
#endif
    ngx_null_command
};

static ngx_http_module_t ngx_http_kafka_log_module_ctx = {
    NULL,                                  /* preconfiguration */
    ngx_http_kafka_log_post_config,         /* postconfiguration */
    ngx_http_kafka_log_create_main_conf,    /* create main configuration */
    NULL,                                  /* init main configuration */
    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */
    ngx_http_kafka_log_create_loc_conf,     /* create location configuration */
    NULL                                   /* merge location configuration */
};

ngx_module_t ngx_http_kafka_log_module = {
    NGX_MODULE_V1,
    &ngx_http_kafka_log_module_ctx,         /* module context */
    ngx_http_kafka_log_commands,            /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_http_kafka_log_init_worker,         /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};

#if (NGX_HAVE_LIBRDKAFKA)
typedef struct {
    ngx_str_node_t node;
    rd_kafka_topic_t *rkt;
} ngx_http_kafka_log_topic_t;

static rd_kafka_topic_t *
ngx_http_kafka_log_get_topic(ngx_http_kafka_log_main_conf_t* conf, ngx_str_t* topic_name)
{
    ngx_http_kafka_log_topic_t* topic;
    rd_kafka_topic_conf_t *rktc;
    ngx_pool_t* pool = ngx_cycle->pool;
    uint32_t hash;

    hash = ngx_crc32_long(topic_name->data, topic_name->len);

    // look up by topic name
    topic = (ngx_http_kafka_log_topic_t *)
        ngx_str_rbtree_lookup(&conf->kafka.topics_rbtree, topic_name, hash);
    if (topic != NULL) {
        return topic->rkt;
    }

    // allocate a new topic object
    topic = ngx_pcalloc(pool, sizeof(*topic));
    if (topic == NULL) {
        return NULL;
    }

    topic->node.str.len = topic_name->len;
    topic->node.str.data = ngx_pstrdup(pool, topic_name);
    if (topic->node.str.data == NULL) {
        return NULL;
    }
    topic->node.node.key = hash;

    ngx_rbtree_insert(&conf->kafka.topics_rbtree, &topic->node.node);

    // create topic conf
    rktc = ngx_kafka_log_kafka_topic_conf_new(pool);
    if (!rktc) {
        return NULL;
    }

    // disable topic acks
    ngx_kafka_log_kafka_topic_disable_ack(pool, rktc);

    topic->rkt = ngx_kafka_log_kafka_topic_new(pool,
        conf->kafka.rk,
        rktc,
        topic_name);
    return topic->rkt;
}
#endif

static ngx_int_t 
ngx_http_kafka_log_log_handler(ngx_http_request_t *r) {

    ngx_http_kafka_log_loc_conf_t        *lc;
    ngx_str_t                           topic;
    ngx_str_t                           txt;
    size_t                              i;
    ngx_kafka_log_output_location_t     *arr;
    ngx_kafka_log_output_location_t     *location;
    rd_kafka_topic_t                    *rkt;

#if (NGX_HAVE_LIBRDKAFKA)
    int                                 err;
    ngx_http_kafka_log_main_conf_t       *mcf;
    ngx_str_t                           msg_id;
#endif

    lc = ngx_http_get_module_loc_conf(r, ngx_http_kafka_log_module);

    /* Bypass if number of location is empty */
    if (!lc->locations->nelts) {
        return NGX_OK;
    }

    /* Discard connect methods ... file is not open!?. Proxy mode  */
    if (r->method == NGX_HTTP_UNKNOWN &&
        ngx_strncasecmp((u_char *)"CONNECT", r->request_line.data, 7) == 0) {
        return NGX_OK;
    }

#if (NGX_HAVE_LIBRDKAFKA)
    mcf = ngx_http_get_module_main_conf(r, ngx_http_kafka_log_module);
#endif

    arr = lc->locations->elts;
    for (i = 0; i < lc->locations->nelts; ++i) {

        location = &arr[i];

        /* get message text */
        if (ngx_http_complex_value(r, &location->cv, &txt) != NGX_OK) {
            continue;
        }

        if (txt.len == 0) {
            ngx_log_error(NGX_LOG_ERR, r->pool->log, 0,
                "ignoring empty message");
            continue;
        }

        /* Write to file */
        if (location->type == NGX_KAFKA_LOG_SINK_FILE) {

            if (!location->file) {
                continue;
            }

            ngx_kafka_log_write_sink_file(
                r->pool->log,
                location->file,
                &txt);
            continue;
        }

#if (NGX_HAVE_LIBRDKAFKA)
        /* Write to kafka */
        if (location->type == NGX_KAFKA_LOG_SINK_KAFKA) {

            if (ngx_http_complex_value(r, &location->kafka.topic, &topic) != NGX_OK) {
                continue;
            }

            if (topic.len == 0) {
                continue;
            }

            rkt = ngx_http_kafka_log_get_topic(mcf, &topic);
            if (rkt == NULL) {
                continue;
            }

            if (location->kafka.http_msg_id_var) {
                if (ngx_http_complex_value(r,
                        location->kafka.http_msg_id_var,
                        &msg_id) != NGX_OK)
                {
                    continue;
                }

                if (msg_id.len == 0) {
                    msg_id.data = NULL;        // must set the data to null since librdkafka uses it to know if a key was passed
                }

                ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->pool->log, 0,
                    "http_kafka_log: kafka msg-id:[%v] msg:[%V]",
                    &msg_id, &txt);
            } else {
                msg_id.data = NULL;
                msg_id.len = 0;
            }

            /* FIXME : Reconnect support */
            /* Send/Produce message. */
            if ((err =  rd_kafka_produce(
                            rkt,
                            mcf->kafka.partition,
                            RD_KAFKA_MSG_F_COPY,
                            txt.data,
                            txt.len,
                            (const char *) msg_id.data,
                            msg_id.len,
                            NULL)) == -1) {

                const char *errstr = rd_kafka_err2str(rd_kafka_errno2err(err));

                ngx_log_error(NGX_LOG_ERR, r->pool->log, 0,
                        "failed to produce to topic %s "
                        "partition %i: %s\n",
                        rd_kafka_topic_name(rkt),
                        mcf->kafka.partition,
                        errstr);
            } else {
                ngx_log_debug3(NGX_LOG_DEBUG_HTTP, r->pool->log, 0,
                    "http_kafka_log: kafka msg:[%V] ERR:[%d] QUEUE:[%d]",
                    &txt, err, rd_kafka_outq_len(mcf->kafka.rk));
            }

            rd_kafka_poll(mcf->kafka.rk, 0);

        } // if KAFKA type
#endif
    } // for location

    return NGX_OK;
}

static ngx_int_t
ngx_http_kafka_log_post_config(ngx_conf_t *cf) {

    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_HTTP_LOG_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }
    *h = ngx_http_kafka_log_log_handler;

    return NGX_OK;
}

static void *
ngx_http_kafka_log_create_main_conf(ngx_conf_t *cf) {

    ngx_http_kafka_log_main_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_kafka_log_main_conf_t));
    if (conf == NULL) {
        return NULL;
    }

#if (NGX_HAVE_LIBRDKAFKA)
    ngx_rbtree_init(&conf->kafka.topics_rbtree, &conf->kafka.topics_sentinel, ngx_str_rbtree_insert_value);

    if (ngx_kafka_log_init_kafka(cf->pool, &conf->kafka) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "http_kafka_log: error initialize kafka conf");
    }
#endif

    return conf;
}

static ngx_int_t
ngx_http_kafka_log_init_worker(ngx_cycle_t *cycle)
{
#if (NGX_HAVE_LIBRDKAFKA)
    ngx_http_kafka_log_main_conf_t *conf =
        ngx_http_cycle_get_module_main_conf(cycle, ngx_http_kafka_log_module);;
    ngx_int_t rc;

    rc = ngx_kafka_log_configure_kafka(cycle->pool, &conf->kafka);
    if (rc != NGX_OK) {
        return NGX_ERROR;
    }
#endif
    return NGX_OK;
}

static void *
ngx_http_kafka_log_create_loc_conf(ngx_conf_t *cf) {

    ngx_http_kafka_log_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_kafka_log_loc_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    conf->locations = ngx_array_create(cf->pool, 1,
            sizeof(ngx_kafka_log_output_location_t));
    if (conf->locations == NULL) {
        return NULL;
    }

    return conf;
}

static char *
ngx_http_kafka_log_loc_output(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {

    ngx_http_compile_complex_value_t     ccv;
    ngx_http_kafka_log_loc_conf_t         *lc = conf;
    ngx_kafka_log_output_location_t       *new_location = NULL;
    ngx_str_t                            *args = cf->args->elts;
    ngx_str_t                            *value = NULL;
    ngx_str_t                             location;
    size_t                                prefix_len;

    new_location = ngx_array_push(lc->locations);
    if (new_location == NULL) {
        return NGX_CONF_ERROR;
    }

    ngx_memzero(new_location, sizeof(*new_location));

    value = &args[1];

    if (NGX_KAFKA_LOG_HAS_FILE_PREFIX(value)) {
        new_location->type = NGX_KAFKA_LOG_SINK_FILE;
        prefix_len = NGX_KAFKA_LOG_FILE_OUT_LEN;
#if (NGX_HAVE_LIBRDKAFKA)
    }
    else if (NGX_KAFKA_LOG_HAS_KAFKA_PREFIX(value)) {
        new_location->type = NGX_KAFKA_LOG_SINK_KAFKA;
        prefix_len = NGX_KAFKA_LOG_KAFKA_OUT_LEN;
#endif
    } else {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "Invalid prefix [%v] for kafka_log", value);
        return NGX_CONF_ERROR;
    }

    /* compile the message body */
    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

    ccv.cf = cf;
    ccv.value = &args[2];
    ccv.complex_value = &new_location->cv;
    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    /* saves location without prefix */
    location       = args[1];
    location.len   -= prefix_len;
    location.data  += prefix_len;

    /* if sink type is file, then try to open it and save */
    if (new_location->type == NGX_KAFKA_LOG_SINK_FILE) {
        new_location->file = ngx_conf_open_file(cf->cycle,
                &location);
    }

#if (NGX_HAVE_LIBRDKAFKA)
    /* if sink type is kafka, then set topic config for this location */
    else if (new_location->type == NGX_KAFKA_LOG_SINK_KAFKA) {

        /* compile topic name */
        ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

        ccv.cf = cf;
        ccv.value = &location;
        ccv.complex_value = &new_location->kafka.topic;
        if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
            return NGX_CONF_ERROR;
        }

        if (cf->args->nelts >= 4)
        {
            /* compile message id */
            ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

            ccv.cf = cf;
            ccv.value = &args[3];
            ccv.complex_value = ngx_pcalloc(cf->pool, sizeof(*ccv.complex_value));
            if (ccv.complex_value == NULL) {
                return NGX_CONF_ERROR;
            }
            if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
                return NGX_CONF_ERROR;
            }
            new_location->kafka.http_msg_id_var = ccv.complex_value;
        }
    }
#endif

    return NGX_CONF_OK;
}
