#include "ngx_kafka_log_output.h"

ngx_int_t
ngx_kafka_log_write_sink_file(
    ngx_log_t *log,
    ngx_open_file_t* file,
    ngx_str_t* txt)
{
    ssize_t n;

    n = ngx_write_fd(file->fd, (u_char *)txt->data, txt->len);
    if (n == -1) {
        ngx_log_error(NGX_LOG_ALERT, log, ngx_errno,
                      ngx_write_fd_n " to \"%s\" failed",
                      file->name.data);
        return NGX_ERROR;

    } else if ((size_t) n != txt->len) {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      ngx_write_fd_n " to \"%s\" was incomplete: %z of %uz",
                      file->name.data, n, txt->len);
        return NGX_ERROR;
    }
    return NGX_OK;
}

ngx_flag_t
ngx_kafka_log_rate_limit(ngx_rate_limit_ctx_t* ctx)
{
    time_t now = ngx_time();

    if (now < ctx->reset_time || 
        now >= ctx->reset_time + ctx->interval) {
        ctx->reset_time = now;
        ctx->left = ctx->limit;
    }

    if (ctx->left <= 0) {
        return 0;
    }

    ctx->left--;
    return 1;
}
