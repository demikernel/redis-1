/*
 * Copyright (c) 2022, Microsoft Corporation
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "connhelpers.h"
#include <demi/sga.h>
#include <demi/wait.h>

ConnectionType CT_DemiSocket;

connection *connCreateDemikernelSocket() {
    connection *conn = zcalloc(sizeof(connection));
    conn->type = &CT_DemiSocket;
    conn->fd = -1;
    conn->private_data = zcalloc(sizeof(demi_qresult_t));
    return conn;
}

connection *connCreateAcceptedDemiQ(int fd) {
    connection *conn = connCreateDemikernelSocket();
    conn->fd = fd;
    conn->state = CONN_STATE_ACCEPTING;
    
    return conn;
}

connection *connCreateListeningSocket(int fd) {
    connection *conn = connCreateDemikernelSocket();
    conn->fd = fd;
    conn->state = CONN_STATE_LISTENING;
    return conn;
}

static int demiSocketConnect(connection *conn, const char *addr, int port, const char *src_addr,
                             ConnectionCallbackFunc connect_handler) {
    int fd = anetTcpNonBlockBestEffortBindConnect(NULL,addr,port,src_addr);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTING;

    conn->conn_handler = connect_handler;
    aeCreateFileEvent(server.el, conn->fd, AE_WRITABLE,
                      conn->type->ae_handler, conn);

    return C_OK;
}

static void demiSocketClose(connection *conn) {
    if (conn->fd != -1) {
        aeDeleteFileEvent(server.el,conn->fd, AE_READABLE | AE_WRITABLE);
        close(conn->fd);
        conn->fd = -1;
    }

    /* If called from within a handler, schedule the close but
     * keep the connection until the handler returns.
     */
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }

    zfree(conn);
}

static int demiSocketWrite(connection *conn, const void *data, size_t data_len) {
    demi_sgarray_t sga = demi_sgaalloc(data_len);
    demi_qtoken_t qt;
    demi_qresult_t qr;
    int ret;
    
    memcpy(sga.sga_segs[0].sgaseg_buf, data, data_len);

    if (((ret = demi_push(&qt, conn->fd, &sga)) != 0 ||
         (ret = demi_wait(&qr, qt)) != 0 ||
         qr.qr_opcode != DEMI_OPC_PUSH) &&
        ret != EAGAIN) {
        conn->last_errno = ret;
        return -1;
    }

    demi_sgafree(&sga);
    return data_len;
}

static int demiSocketWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    size_t data_len = 0;
    demi_sgarray_t sga;
    demi_qtoken_t qt;
    demi_qresult_t qr;
    int ret;

    printf("Sending ev: \n");

    for (int i = 0; i < iovcnt; i++) {
        data_len += iov[i].iov_len;
    }
    sga = demi_sgaalloc(data_len);

    char *offset = (char *)sga.sga_segs[0].sgaseg_buf;
    for (int i = 0; i < iovcnt; i++) {
        memcpy(offset, iov[i].iov_base, iov[i].iov_len);
        offset += iov[i].iov_len;
    }
    
    if (((ret = demi_push(&qt, conn->fd, &sga)) != 0 ||
         (ret = demi_wait(&qr, qt)) != 0) &&
        errno != EAGAIN) {
        conn->last_errno = ret;
        return -1;
    }
    demi_sgafree(&sga);
    return data_len;
}

static int demiSocketRead(connection *conn, void *buf, size_t buf_len) {
    /* We're storing the result from the last wait in a global variable */
    demi_qresult_t *qr = &recent_qr;
    UNUSED(conn);

    if (qr->qr_value.sga.sga_segs[0].sgaseg_len == 0 ||
        qr->qr_value.sga.sga_segs[0].sgaseg_buf == NULL ||
        qr->qr_opcode != DEMI_OPC_POP) {
        //        conn->state = CONN_STATE_CLOSED;
        return 0;
    }
    /* we can't do more sophisticated error handling yet
    /* else if (ret < 0 && errno != EAGAIN) { */
    /*     conn->last_errno = errno; */

    /*     /\* Don't overwrite the state of a connection that is not already */
    /*      * connected, not to mess with handler callbacks. */
    /*      *\/ */
    /*     if (errno != EINTR && conn->state == CONN_STATE_CONNECTED) */
    /*         conn->state = CONN_STATE_ERROR; */
    /* } */

    /* Irene: Assume only one scatter gather element */
    size_t read_len = qr->qr_value.sga.sga_segs[0].sgaseg_len;
    if (read_len > buf_len) {
        // panic?
    } else {
        memcpy(buf, qr->qr_value.sga.sga_segs[0].sgaseg_buf, read_len);
    }
    //Irene: Use memory freely for debugging
    //demi_sgafree(&qr->qr_value.sga);
    return read_len;
}

static int demiSocketAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    int ret = C_OK;

    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;
    conn->state = CONN_STATE_CONNECTED;

    connIncrRefs(conn);
    if (!callHandler(conn, accept_handler)) ret = C_ERR;
    connDecrRefs(conn);

    return ret;
}

/* We probably don't need a Demikernel specific function but the conn
   function is not available here */ 
static int demiSocketSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    if (func == conn->write_handler) return C_OK;

    conn->write_handler = func;
    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    if (!conn->write_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,AE_WRITABLE,
                    conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

static int demiSocketSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    if (func == conn->read_handler) return C_OK;

    conn->read_handler = func;
    if (!conn->read_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_READABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,
                    AE_READABLE,conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

static const char *demiSocketGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

static void demiSocketEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask)
{
    UNUSED(fd);
    connection *conn = clientData;
    //recent_qr = &el->fired[0].qr;
    if (conn->state == CONN_STATE_CONNECTING &&
            (mask & AE_WRITABLE) && conn->conn_handler) {

        int conn_error = connGetSocketError(conn);
        if (conn_error) {
            conn->last_errno = conn_error;
            conn->state = CONN_STATE_ERROR;
        } else {
            conn->state = CONN_STATE_CONNECTED;
        }

        if (!conn->write_handler) aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);

        if (!callHandler(conn, conn->conn_handler)) return;
        conn->conn_handler = NULL;
    }
    int invert = conn->flags & CONN_FLAG_WRITE_BARRIER;

    int call_write = (mask & AE_WRITABLE) && conn->write_handler;
    int call_read = (mask & AE_READABLE) && conn->read_handler;

    /* Handle normal I/O flows */
    if (!invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
    /* Fire the writable event. */
    if (call_write) {
        if (!callHandler(conn, conn->write_handler)) return;
    }
    /* If we have to invert the call, fire the readable event now
     * after the writable one. */
    if (invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
}

static int demiSocketBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
    int fd = anetTcpNonBlockConnect(NULL,addr,port);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    if ((aeWait(fd, AE_WRITABLE, timeout) & AE_WRITABLE) == 0) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = ETIMEDOUT;
    }

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTED;
    return C_OK;
}

/* Not sure if we need these. Just panic for now */

static ssize_t demiSocketSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    //    panic("demiConn: demiSocketSyncWrite not supported!");
    UNUSED(conn);
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(timeout);
    return 0; //syncWrite(conn->fd, ptr, size, timeout);
}

static ssize_t demiSocketSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    //panic("demiConn: demiSocketSyncRead not supported!");
    UNUSED(conn);
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(timeout);
    return 0;// syncRead(conn->fd, ptr, size, timeout);
}

static ssize_t demiSocketSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    //panic("demiConn: demiSocketReadLine not supported!");
    UNUSED(conn);
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(timeout);
    return 0; //syncReadLine(conn->fd, ptr, size, timeout);
}

static int connSocketGetType(connection *conn) {
    (void) conn;

    return CONN_TYPE_DEMIKERNEL;
}

ConnectionType CT_DemiSocket = {
    .ae_handler = demiSocketEventHandler,
    .close = demiSocketClose,
    .write = demiSocketWrite,
    .writev = demiSocketWritev,
    .read = demiSocketRead,
    .accept = demiSocketAccept,
    .connect = demiSocketConnect,
    .set_write_handler = demiSocketSetWriteHandler,
    .set_read_handler = demiSocketSetReadHandler,
    .get_last_error = demiSocketGetLastError,
    .blocking_connect = demiSocketBlockingConnect,
    .sync_write = demiSocketSyncWrite,
    .sync_read = demiSocketSyncRead,
    .sync_readline = demiSocketSyncReadLine,
    .get_type = connSocketGetType
};


int demiGetSocketError(connection *conn) {
    int sockerr = 0;
    socklen_t errlen = sizeof(sockerr);

    // Irene: Do we have get sockopt?
    if (getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    return sockerr;
}
