/* Demikernel wait based ae.c module
 *
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

#include <demi/wait.h>
#include <demi/libos.h>
#include <stdbool.h>

int IS_DEMIKERNEL_SERVER = 0;

typedef struct aeApiState {
    /* map of fds to mask */
    int *fd_mask_map;
    /* number of qtokens actively in use */
    size_t num_qtokens;
    /* list of active qtokens */
    demi_qtoken_t *qtokens;
    /* mapping from file descriptor to offset in the qtoken array */
    int *fd_to_qtoken;
    /* mapping from qtoken array to file descriptor*/
    int *qtoken_to_fd;
    /* fd's with a writable event */
    int *writable_fd_list;
    size_t num_writable_fds;    
} aeApiState;

static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    /* allocate queue descriptor tracking */
    state->fd_to_qtoken = zcalloc(sizeof(int) * eventLoop->setsize);
    state->writable_fd_list = zcalloc(sizeof(int) * eventLoop->setsize);
    state->fd_mask_map = zcalloc(sizeof(bool) * eventLoop->setsize);
    if (state->fd_to_qtoken == NULL ||
        state->writable_fd_list == NULL ||
        state->fd_mask_map == NULL) {
        zfree(state->fd_to_qtoken);
        zfree(state->writable_fd_list);
        zfree(state->fd_mask_map);
        zfree(state);
        return -1;
    }
    state->num_qtokens = 0;
    state->qtokens = NULL;
    state->qtoken_to_fd = NULL;
    state->num_writable_fds = 0;
    eventLoop->apidata = state;
    return 0;
}

static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;

    /* adjust max queue descriptor tracking, got this from ae_epoll
       zmalloc comment says that the array gets zero'd but I think it
       is not */
    state->fd_to_qtoken = zrealloc(state->fd_to_qtoken, sizeof(int) * setsize);
    state->writable_fd_list = zrealloc(state->writable_fd_list, sizeof(int) * setsize);
    state->fd_mask_map = zrealloc(state->fd_mask_map, sizeof(bool) * setsize);
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    zfree(state->qtokens);
    zfree(state->fd_to_qtoken);
    zfree(state->qtoken_to_fd);
    zfree(state->writable_fd_list);
    zfree(state->fd_mask_map);
    zfree(state);
}

static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    /* we only allow one event of each type so, check if some of the
       bits are already set */
    if ((mask & AE_READABLE) && !(state->fd_mask_map[fd] & AE_READABLE)) {
        demi_qtoken_t qt = 1110; // for debugging
        int ret = 0;
        if (IS_DEMIKERNEL_SERVER)
        {
            /* BIG HACK: we always use the first queue descriptor for
            listening, so we know to call accept instead of pop */
            if (fd == 0)
                ret = demi_accept(&qt, fd);
            else ret = demi_pop(&qt, fd);
        }
        else ret = demi_pop(&qt, fd);

        if (ret != 0) return -1;

        /* increase the number of qtokens that we are tracking now */
        int i = state->num_qtokens++;

        /* increase the size of the qtoken arrays */
        state->qtokens = zrealloc(state->qtokens, sizeof(demi_qtoken_t) * state->num_qtokens);
        state->qtoken_to_fd = zrealloc(state->qtoken_to_fd, sizeof(int) * state->num_qtokens);
        /* place the qtoken at the end */
        state->qtokens[i] = qt;
        /* update our map */
        state->qtoken_to_fd[i] = fd;
        state->fd_to_qtoken[fd] = i;
    }

    if ((mask & AE_WRITABLE) && !(state->fd_mask_map[fd] & AE_WRITABLE)) {
	int i = state->num_writable_fds;

	state->num_writable_fds += 1;
	state->writable_fd_list[i] = fd;
    }

    state->fd_mask_map[fd] = state->fd_mask_map[fd] | mask;
    //printf("Add (mask=%u): num read events=%lu num write events=%lu\n", mask, state->num_qtokens, state->num_writable_fds);
    return 0;
}

static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask) {
    aeApiState *state = eventLoop->apidata;
    if ((delmask & AE_READABLE) && (state->fd_mask_map[fd] & AE_READABLE)) {
        /* grab the offset of the qtoken that we are removing */
        int i = state->fd_to_qtoken[fd];
        /* zero it out since we are no longer using it */
        state->fd_to_qtoken[fd] = 0;

        /* update the size of the qtoken array and grab the index of
           the last qtoken to move it into the empty space*/
        int moving_qtoken = --state->num_qtokens;
        if (i != moving_qtoken) {
            /* move last element to the one that we are removing
               (unless it already is the last element)*/
            state->qtokens[i] = state->qtokens[moving_qtoken];
            /* update our mappings */
            int moving_fd = state->qtoken_to_fd[moving_qtoken];
            state->qtoken_to_fd[i] = moving_fd;
            state->fd_to_qtoken[moving_fd] = i;
        }
        /* resize the qtoken arrays */
        state->qtokens = zrealloc(state->qtokens, sizeof(demi_qtoken_t) * state->num_qtokens);
        state->qtoken_to_fd = zrealloc(state->qtoken_to_fd, sizeof(int) * state->num_qtokens);        
    }

    if ((delmask & AE_WRITABLE) && (state->fd_mask_map[fd] & AE_WRITABLE)) {
        int last_index = --state->num_writable_fds;
        for (int i = 0; i < last_index; i++) {
            if (state->writable_fd_list[i] == fd) {
                state->writable_fd_list[i] = state->writable_fd_list[last_index];
            }
        }
    }
    state->fd_mask_map[fd] = state->fd_mask_map[fd] & (~delmask);
    //printf("Delete(mask=%u): num read events=%lu num write events=%lu\n", delmask, state->num_qtokens, state->num_writable_fds);
}

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    /* Need to stop ignoring tvp once we support wait_any with a time out */
    
    aeApiState *state = eventLoop->apidata;
    int retval = 0, ready_offset = 100;
    demi_qresult_t *qr = &recent_qr;

    if (state->num_writable_fds > 0) {
        eventLoop->fired[0].fd = state->writable_fd_list[0];
        eventLoop->fired[0].mask = AE_WRITABLE;
    } else if (state->num_qtokens > 0) {
        retval = demi_wait_any(qr, &ready_offset, state->qtokens, state->num_qtokens);

	/* demi_wait_any only returns one event at a time */
	if (retval == 0) {
	    int mask = state->fd_mask_map[qr->qr_qd];
	    demi_qtoken_t qt = 100; // for debugging
	    if (qr->qr_opcode == DEMI_OPC_POP) {
		/* if no buffer is returned, then there was an error */            
		if (qr->qr_value.sga.sga_segs[0].sgaseg_len == 0 ||
		    qr->qr_value.sga.sga_segs[0].sgaseg_buf == NULL ||
		    qr->qr_opcode == 5) {
		    state->qtokens[ready_offset] = 0;
		} else {
		    retval = demi_pop(&qt, qr->qr_qd);
		    state->qtokens[ready_offset] = qt;
		}
	    } else if (qr->qr_opcode == DEMI_OPC_ACCEPT) {
		retval = demi_accept(&qt, qr->qr_qd);
		state->qtokens[ready_offset] = qt;
	    }
	    if (retval != 0) {
		/* Not sure if this is the right way to indicate an error */            
		panic("aeApiPoll: waitany, %s", strerror(retval));
	    }
	    eventLoop->fired[0].fd = qr->qr_qd;
	    eventLoop->fired[0].mask = mask;
	} else {
	    panic("aeApiPoll: waitany, %s", strerror(retval));
	}
    } else {
	panic("aeApiPoll: no events!");
    }
    return 1;
}

static char *aeApiName(void) {
    return "epoll";
}
