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
#include "ngx_kafka_log_kafka.h"

#if (NGX_HAVE_LIBRDKAFKA)

#include "ngx_kafka_log_output.h"

#define NGX_KAFKA_LOG_KAFKA_ERROR_MSG_LEN (2048)

static ngx_rate_limit_ctx_t error_rate_limit = ngx_rate_limit(1, 4);

static char *
ngx_kafka_log_str_dup(ngx_pool_t *pool, ngx_str_t *src)
{
    char *dst;

    dst = ngx_pnalloc(pool, src->len + 1);
    if (dst == NULL) {
        return NULL;
    }

    ngx_memcpy(dst, src->data, src->len);
    dst[src->len] = '\0';
    return dst;
}

rd_kafka_topic_conf_t *
ngx_kafka_log_kafka_topic_conf_new(ngx_pool_t *pool)
{
    rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    if (!topic_conf) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
                "kafka_log: rd_kafka_topic_conf_new failed");
    }
    return topic_conf;
}

static rd_kafka_conf_t *
ngx_kafka_log_kafka_conf_new(ngx_pool_t *pool)
{
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!conf) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
                "kafka_log: rd_kafka_conf_new failed");
    }
    return conf;
}

static rd_kafka_conf_res_t
ngx_kafka_log_kafka_topic_conf_set_str(
    ngx_pool_t *pool,
    rd_kafka_topic_conf_t *topic_conf,
    const char *key,
    ngx_str_t *str)
{
    char errstr[NGX_KAFKA_LOG_KAFKA_ERROR_MSG_LEN];
    rd_kafka_conf_res_t ret;

    char *value = ngx_kafka_log_str_dup(pool, str);
    if (!value)
    {
        return NGX_ERROR;
    }

    ret = rd_kafka_topic_conf_set(topic_conf, key, value, errstr, sizeof(errstr));
    if (ret != RD_KAFKA_CONF_OK) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
                "kafka_log: rd_kafka_topic_conf_set failed [%s] => [%s] : %s", key, value, errstr);
        return NGX_ERROR;
    }
    return NGX_OK;

}

static void
ngx_kafka_log_kafka_producer_free(void* context)
{
    rd_kafka_destroy((rd_kafka_t *)context);
}

static rd_kafka_t*
ngx_kafka_log_kafka_producer_new(
    ngx_pool_t *pool,
    rd_kafka_conf_t * conf)
{
    ngx_pool_cleanup_t* cln;
    rd_kafka_t *rk = NULL;
    char errstr[NGX_KAFKA_LOG_KAFKA_ERROR_MSG_LEN];

    cln = ngx_pool_cleanup_add(pool, 0);
    if (cln == NULL)
    {
        return NULL;
    }

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
                "kafka_log: rd_kafka_new failed");
        return NULL;
    }

    cln->handler = ngx_kafka_log_kafka_producer_free;
    cln->data = rk;

    return rk;
}

typedef struct {
    rd_kafka_t *rk;
    rd_kafka_topic_t * rkt;
} ngx_kafka_log_cleanup_kafka_topic_t;

static void
ngx_kafka_log_kafka_topic_free(void* data)
{
    ngx_kafka_log_cleanup_kafka_topic_t *c = data;

    rd_kafka_flush(c->rk, 5 * 1000);

    rd_kafka_topic_destroy(c->rkt);
}

rd_kafka_topic_t *
ngx_kafka_log_kafka_topic_new(
    ngx_pool_t *pool,
    rd_kafka_t *rk,
    rd_kafka_topic_conf_t *topic_conf,
    ngx_str_t *topic)
{
    ngx_kafka_log_cleanup_kafka_topic_t* clnt;
    ngx_pool_cleanup_t* cln;
    char *value;

    if (!rk) {
        ngx_log_error(NGX_LOG_CRIT, pool->log, 0,
                "kafka_log: missing kafka handler");
        return NULL;
    }

    cln = ngx_pool_cleanup_add(pool, sizeof(*clnt));
    if (cln == NULL)
    {
        return NULL;
    }

    value = ngx_kafka_log_str_dup(pool, topic);
    if (!value)
    {
        return NULL;
    }

    rd_kafka_topic_t * rkt = rd_kafka_topic_new(rk, value, topic_conf);
    if (!rkt) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
                "kafka_log: rd_kafka_topic_new failed \"%V\"", topic);
        return NULL;
    }

    cln->handler = ngx_kafka_log_kafka_topic_free;
    clnt = cln->data;
    clnt->rk = rk;
    clnt->rkt = rkt;

    return rkt;
}

static void
ngx_kafka_log_kafka_dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    if (rkmessage->err &&
        ngx_kafka_log_rate_limit(&error_rate_limit))
    {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
            "kafka message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
    }
}

static void
ngx_kafka_log_kafka_error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
        "kafka error: %s", reason);
}

ngx_int_t
ngx_kafka_log_init_kafka(
    ngx_pool_t *pool,
    ngx_kafka_log_main_kafka_conf_t *kafka)
{
    kafka->rk = NULL;

    kafka->rkc = ngx_kafka_log_kafka_conf_new(pool);
    if (! kafka->rkc) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

ngx_int_t
ngx_kafka_log_kafka_conf_property_set(
    ngx_pool_t *pool,
    ngx_kafka_log_main_kafka_conf_t *conf,
    ngx_keyval_t *prop)
{
    char                 errstr[NGX_KAFKA_LOG_KAFKA_ERROR_MSG_LEN];
    char                *key, *value;
    rd_kafka_conf_res_t  ret;

    key = ngx_kafka_log_str_dup(pool, &prop->key);
    if (!key) {
        return NGX_ERROR;
    }

    value = ngx_kafka_log_str_dup(pool, &prop->value);
    if (!value) {
        return NGX_ERROR;
    }

    ret = rd_kafka_conf_set(conf->rkc, key, value, errstr, sizeof(errstr));
    if (ret != RD_KAFKA_CONF_OK) {
        ngx_log_error(NGX_LOG_ERR, pool->log, 0,
                     "kafka_log: rd_kafka_conf_set failed [%s] => [%s]: %s",
                      key, value, errstr);
        return NGX_ERROR;
    }

    return NGX_OK;
}

ngx_int_t
ngx_kafka_log_configure_kafka(ngx_pool_t *pool,
        ngx_kafka_log_main_kafka_conf_t *conf) {

    rd_kafka_conf_set_dr_msg_cb(conf->rkc,
        ngx_kafka_log_kafka_dr_msg_cb);

    rd_kafka_conf_set_error_cb(conf->rkc,
        ngx_kafka_log_kafka_error_cb);

    /* create kafka handler */
    conf->rk = ngx_kafka_log_kafka_producer_new(
            pool,
            conf->rkc);
    if (! conf->rk) {
        return NGX_ERROR;
    }

    rd_kafka_set_log_level(conf->rk,
        conf->log_level);

    return NGX_OK;
}

void
ngx_kafka_log_kafka_topic_disable_ack(ngx_pool_t *pool,
        rd_kafka_topic_conf_t *rktc) {

    static const char *conf_req_required_acks_key  = "request.required.acks";
    static ngx_str_t   conf_zero_value             = ngx_string("0");

    ngx_kafka_log_kafka_topic_conf_set_str(pool, rktc,
            conf_req_required_acks_key, &conf_zero_value);
}
#endif// NGX_HAVE_LIBRDKAFKA
