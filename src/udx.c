#include <assert.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <uv.h>

#include "../include/udx.h"

#include "cirbuf.h"
#include "endian.h"
#include "fifo.h"
#include "io.h"
#include "debug.h"

#define UDX_STREAM_ALL_DESTROYED (UDX_STREAM_DESTROYED | UDX_STREAM_DESTROYED_REMOTE)
#define UDX_STREAM_ALL_ENDED (UDX_STREAM_ENDED | UDX_STREAM_ENDED_REMOTE)
#define UDX_STREAM_DEAD (UDX_STREAM_ALL_DESTROYED | UDX_STREAM_DESTROYING | UDX_STREAM_CLOSED)

#define UDX_STREAM_SHOULD_READ (UDX_STREAM_ENDED_REMOTE | UDX_STREAM_DEAD)
#define UDX_STREAM_READ 0

#define UDX_STREAM_SHOULD_END (UDX_STREAM_ENDING | UDX_STREAM_ENDED | UDX_STREAM_DEAD)
#define UDX_STREAM_END UDX_STREAM_ENDING

#define UDX_STREAM_SHOULD_END_REMOTE (UDX_STREAM_ENDED_REMOTE | UDX_STREAM_DEAD | UDX_STREAM_ENDING_REMOTE)
#define UDX_STREAM_END_REMOTE UDX_STREAM_ENDING_REMOTE

#define UDX_PACKET_CALLBACK (UDX_PACKET_STREAM_SEND | UDX_PACKET_STREAM_DESTROY | UDX_PACKET_SEND)
#define UDX_PACKET_FREE_ON_SEND (UDX_PACKET_STREAM_STATE | UDX_PACKET_STREAM_DESTROY)

#define UDX_HEADER_DATA_OR_END (UDX_HEADER_DATA | UDX_HEADER_END)

#define UDX_DEFAULT_TTL 64

#define UDX_MAX_TRANSMITS 5

typedef struct {
  uint32_t seq; // must be the first entry, so its compat with the cirbuf

  int type;

  uv_buf_t buf;
} udx_pending_read_t;

static uint64_t
get_microseconds () {
  return uv_hrtime() / 1000;
}

static uint64_t
get_milliseconds () {
  return get_microseconds() / 1000;
}

static uint32_t
max_uint32 (uint32_t a, uint32_t b) {
  return a < b ? b : a;
}

static int32_t
seq_diff (uint32_t a, uint32_t b) {
  return a - b;
}

static int
seq_compare (uint32_t a, uint32_t b) {
  int32_t d = seq_diff(a, b);
  return d < 0 ? -1 : d > 0 ? 1 : 0;
}

static void
on_uv_poll (uv_poll_t *handle, int status, int events);

static void
ref_inc (udx_t *udx) {
  udx->refs++;

  if (udx->streams != NULL) return;

  udx->streams_len = 0;
  udx->streams_max_len = 16;
  udx->streams = malloc(udx->streams_max_len * sizeof(udx_stream_t *));

  udx__cirbuf_init(&(udx->streams_by_id), 16);
}

static void
ref_dec (udx_t *udx) {
  udx->refs--;

  if (udx->refs || udx->streams == NULL) return;

  free(udx->streams);
  udx->streams = NULL;
  udx->streams_max_len = 0;

  udx__cirbuf_destroy(&(udx->streams_by_id));
}

static void
trigger_socket_close (udx_socket_t *socket) {
  if (--socket->pending_closes) return;

  if (socket->on_close != NULL) {
    socket->on_close(socket);
  }

  ref_dec(socket->udx);
}

static void
on_uv_close (uv_handle_t *handle) {
  trigger_socket_close((udx_socket_t *) handle->data);
}

static void
on_uv_interval (uv_timer_t *handle) {
  udx_check_timeouts((udx_t *) handle->data);
}

static int
udx_start_timer (udx_t *udx) {
  uv_timer_t *timer = &(udx->timer);

  memset(timer, 0, sizeof(uv_timer_t));

  int err = uv_timer_init(udx->loop, timer);
  assert(err == 0);

  err = uv_timer_start(timer, on_uv_interval, UDX_CLOCK_GRANULARITY_MS, UDX_CLOCK_GRANULARITY_MS);
  assert(err == 0);

  timer->data = udx;

  return err;
}

static void
on_udx_timer_close (uv_handle_t *handle) {
  udx_t *udx = (udx_t *) handle->data;
  udx_socket_t *socket = udx->timer_closed_by;

  if (udx->sockets > 0) { // re-open
    udx->timer_closed_by = NULL;
    udx_start_timer(udx);
  }

  trigger_socket_close(socket);
}

static void
close_handles (udx_socket_t *handle) {
  if (handle->status & UDX_SOCKET_CLOSING_HANDLES) return;
  handle->status |= UDX_SOCKET_CLOSING_HANDLES;

  if (handle->status & UDX_SOCKET_BOUND) {
    handle->pending_closes++;
    uv_poll_stop(&(handle->io_poll));
    uv_close((uv_handle_t *) &(handle->io_poll), on_uv_close);
  }

  handle->pending_closes += 2; // one below and one in trigger_socket_close
  uv_close((uv_handle_t *) &(handle->socket), on_uv_close);

  udx_t *udx = handle->udx;

  udx->sockets--;

  if (udx->sockets > 0 || udx->timer_closed_by) {
    trigger_socket_close(handle);
    return;
  }

  udx->timer_closed_by = handle;

  uv_timer_stop(&(udx->timer));
  uv_close((uv_handle_t *) &(udx->timer), on_udx_timer_close);
}

static int
update_poll (udx_socket_t *socket) {
  int events = UV_READABLE;

  if (socket->send_queue.len > 0) {
    events |= UV_WRITABLE;
  }

  if (events == socket->events) return 0;

  socket->events = events;
  return uv_poll_start(&(socket->io_poll), events, on_uv_poll);
}

static void
clear_incoming_packets (udx_stream_t *stream) {
  uint32_t seq = stream->ack;
  udx_cirbuf_t *inc = &(stream->incoming);

  while (stream->pkts_buffered) {
    udx_pending_read_t *pkt = (udx_pending_read_t *) udx__cirbuf_remove(inc, seq++);
    if (pkt == NULL) continue;

    stream->pkts_buffered--;
    free(pkt);
  }
}

static void
clear_outgoing_packets (udx_stream_t *stream) {
  // We should make sure all existing packets do not send, and notify the user that they failed
  for (uint32_t seq = stream->remote_acked; seq != stream->seq; seq++) {
    udx_packet_t *pkt = (udx_packet_t *) udx__cirbuf_remove(&(stream->outgoing), seq);

    if (pkt == NULL) continue;

    // Make sure to remove it from the fifo, if it was added
    if (pkt->status == UDX_PACKET_SENDING) {
      udx__fifo_remove(&(stream->socket->send_queue), pkt, pkt->fifo_gc);
    }

    udx_stream_write_t *w = (udx_stream_write_t *) pkt->ctx;

    if (--(w->packets) == 0) {
      if (w->on_ack != NULL) {
        w->on_ack(w, UV_ECANCELED, 0);
      }
    }

    free(pkt);
  }
}

static void
init_stream_packet (udx_packet_t *pkt, int type, udx_stream_t *stream, const uv_buf_t *buf) {
  uint8_t *b = (uint8_t *) &(pkt->header);

  // 8 bit magic byte + 8 bit version + 8 bit type + 8 bit extensions
  *(b++) = UDX_MAGIC_BYTE;
  *(b++) = UDX_VERSION;
  *(b++) = (uint8_t) type;
  *(b++) = 0; // data offset

  uint32_t *i = (uint32_t *) b;

  // 32 bit (le) remote id
  *(i++) = udx__swap_uint32_if_be(stream->remote_id);
  // 32 bit (le) recv window
  *(i++) = 0xffffffff; // hardcode max recv window
  // 32 bit (le) seq
  *(i++) = udx__swap_uint32_if_be(stream->seq);
  // 32 bit (le) ack
  *(i++) = udx__swap_uint32_if_be(stream->ack);

  pkt->seq = stream->seq;
  pkt->is_retransmit = 0;
  pkt->transmits = 0;
  pkt->size = (uint16_t) (UDX_HEADER_SIZE + buf->len);
  pkt->dest = stream->remote_addr;

  pkt->bufs_len = 2;

  pkt->bufs[0] = uv_buf_init((char *) &(pkt->header), UDX_HEADER_SIZE);
  pkt->bufs[1] = *buf;
}

static int
send_state_packet (udx_stream_t *stream) {
  if ((stream->status & UDX_STREAM_CONNECTED) == 0) return 0;

  uint32_t *sacks = NULL;
  uint32_t start = 0;
  uint32_t end = 0;

  udx_packet_t *pkt = NULL;

  void *payload = NULL;
  size_t payload_len = 0;

  int ooo = stream->out_of_order;

  // 65536 is just a sanity check here in terms of how much max work we wanna do, could prob be smarter
  // only valid if ooo is very large
  for (uint32_t i = 0; i < 65536 && ooo > 0 && payload_len < 400; i++) {
    uint32_t seq = stream->ack + 1 + i;
    if (udx__cirbuf_get(&(stream->incoming), seq) == NULL) continue;

    ooo--;

    if (sacks == NULL) {
      pkt = malloc(sizeof(udx_packet_t) + 1024);
      payload = (((char *) pkt) + sizeof(udx_packet_t));
      sacks = (uint32_t *) payload;
      start = seq;
      end = seq + 1;
    } else if (seq == end) {
      end++;
    } else {
      *(sacks++) = udx__swap_uint32_if_be(start);
      *(sacks++) = udx__swap_uint32_if_be(end);
      start = seq;
      end = seq + 1;
      payload_len += 8;
    }
  }

  if (start != end) {
    *(sacks++) = udx__swap_uint32_if_be(start);
    *(sacks++) = udx__swap_uint32_if_be(end);
    payload_len += 8;
  }

  if (pkt == NULL) pkt = malloc(sizeof(udx_packet_t));

  uv_buf_t buf = uv_buf_init(payload, payload_len);

  init_stream_packet(pkt, payload ? UDX_HEADER_SACK : 0, stream, &buf);

  pkt->status = UDX_PACKET_SENDING;
  pkt->type = UDX_PACKET_STREAM_STATE;
  pkt->ttl = 0;

  stream->stats_pkts_sent++;

  udx__fifo_push(&(stream->socket->send_queue), pkt);
  return update_poll(stream->socket);
}

static int
send_data_packet (udx_stream_t *stream, udx_packet_t *pkt) {
  if (stream->inflight + pkt->size > stream->cwnd) {
    return 0;
  }

  assert(pkt->status == UDX_PACKET_WAITING);

  pkt->status = UDX_PACKET_SENDING;

  stream->pkts_waiting--;
  stream->pkts_inflight++;
  stream->inflight += pkt->size;

  if (pkt->is_retransmit) {
    pkt->is_retransmit = 0;
    stream->retransmits_waiting--;
  }

  stream->stats_pkts_sent++;
  pkt->fifo_gc = udx__fifo_push(&(stream->socket->send_queue), pkt);

  int err = update_poll(stream->socket);
  return err < 0 ? err : 1;
}

static int
flush_waiting_packets (udx_stream_t *stream) {
  const uint32_t was_waiting = stream->pkts_waiting;
  uint32_t seq = stream->retransmits_waiting ? stream->remote_acked : (stream->seq - stream->pkts_waiting);

  int sent = 0;

  while (seq != stream->seq && stream->pkts_waiting > 0) {
    udx_packet_t *pkt = (udx_packet_t *) udx__cirbuf_get(&(stream->outgoing), seq++);

    if (pkt == NULL || pkt->status != UDX_PACKET_WAITING) continue;

    sent = send_data_packet(stream, pkt);
    if (sent <= 0) break;
  }

  // TODO: retransmits are counted in pkts_waiting, but we (prob) should not count them
  // towards to drain loop - investigate that.
  if (was_waiting > 0 && stream->pkts_waiting == 0 && stream->on_drain != NULL) {
    stream->on_drain(stream);
  }

  if (sent < 0) return sent;
  return 0;
}

static int
close_maybe (udx_stream_t *stream, int err) {
  // if BOTH closed or ANY destroyed.
  if ((stream->status & UDX_STREAM_ALL_ENDED) != UDX_STREAM_ALL_ENDED && !(stream->status & UDX_STREAM_ALL_DESTROYED)) return 0;
  // if we already destroyed, bail.
  if (stream->status & UDX_STREAM_CLOSED) return 0;

  stream->status |= UDX_STREAM_CLOSED;

  udx_t *udx = stream->udx;

  // Remove from the set, by array[i] = array.pop()
  udx_stream_t *other = udx->streams[--(udx->streams_len)];
  udx->streams[stream->set_id] = other;
  other->set_id = stream->set_id;

  udx__cirbuf_remove(&(udx->streams_by_id), stream->local_id);
  clear_incoming_packets(stream);

  // TODO: move the instance to a TIME_WAIT state, so we can handle retransmits

  if (stream->status & UDX_STREAM_READING) {
    udx_stream_read_stop(stream);
  }

  udx__cirbuf_destroy(&(stream->incoming));
  udx__cirbuf_destroy(&(stream->outgoing));

  if (stream->on_close != NULL) {
    stream->on_close(stream, err);
  }

  ref_dec(udx);

  return 1;
}

static int
ack_packet (udx_stream_t *stream, uint32_t seq, int sack) {
  udx_cirbuf_t *out = &(stream->outgoing);
  udx_packet_t *pkt = (udx_packet_t *) udx__cirbuf_remove(out, seq);

  if (pkt == NULL) return 0;

  if (pkt->is_retransmit) {
    pkt->is_retransmit = 0;
    stream->retransmits_waiting--;
    stream->pkts_waiting--;
  }

  if (pkt->status == UDX_PACKET_INFLIGHT) {
    stream->pkts_inflight--;
    stream->inflight -= pkt->size;
  }

  if (pkt->transmits == 1) {
    const uint32_t rtt = (uint32_t) (get_milliseconds() - pkt->time_sent);

    // First round trip time sample
    if (stream->srtt == 0) {
      stream->srtt = rtt;
      stream->rttvar = rtt / 2;
      stream->rto = stream->srtt + max_uint32(UDX_CLOCK_GRANULARITY_MS, 4 * stream->rttvar);
    } else {
      const uint32_t delta = rtt < stream->srtt ? stream->srtt - rtt : rtt - stream->srtt;
      // RTTVAR <- (1 - beta) * RTTVAR + beta * |SRTT - R'| where beta is 1/4
      stream->rttvar = (3 * stream->rttvar + delta) / 4;

      // SRTT <- (1 - alpha) * SRTT + alpha * R' where alpha is 1/8
      stream->srtt = (7 * stream->srtt + rtt) / 8;
    }

    // RTO <- SRTT + max (G, K*RTTVAR) where K is 4 maxed with 1s
    stream->rto = max_uint32(stream->srtt + max_uint32(UDX_CLOCK_GRANULARITY_MS, 4 * stream->rttvar), 1000);
  }

  if (!sack) { // Reset rto timer when new data is ack'ed (inorder)
    stream->rto_timeout = get_milliseconds() + stream->rto;
  }

  // If this packet was queued for sending we need to remove it from the queue.
  if (pkt->status == UDX_PACKET_SENDING) {
    udx__fifo_remove(&(stream->socket->send_queue), pkt, pkt->fifo_gc);
  }

  udx_stream_write_t *w = (udx_stream_write_t *) pkt->ctx;

  free(pkt);

  if (--(w->packets) != 0) return 1;

  if (w->on_ack != NULL) {
    w->on_ack(w, 0, sack);
  }

  if (stream->status & UDX_STREAM_DEAD) return 2;

  // TODO: the end condition needs work here to be more "stateless"
  // ie if the remote has acked all our writes, then instead of waiting for retransmits, we should
  // clear those and mark as local ended NOW.
  if ((stream->status & UDX_STREAM_SHOULD_END) == UDX_STREAM_END && stream->pkts_waiting == 0 && stream->pkts_inflight == 0) {
    stream->status |= UDX_STREAM_ENDED;
    return 2;
  }

  return 1;
}

static void
process_sacks (udx_stream_t *stream, char *buf, size_t buf_len) {
  uint32_t n = 0;
  uint32_t *sacks = (uint32_t *) buf;

  for (size_t i = 0; i + 8 <= buf_len; i += 8) {
    uint32_t start = udx__swap_uint32_if_be(*(sacks++));
    uint32_t end = udx__swap_uint32_if_be(*(sacks++));
    int32_t len = seq_diff(end, start);

    for (int32_t j = 0; j < len; j++) {
      int a = ack_packet(stream, start + j, 1);
      if (a == 2) return; // ended
      if (a == 1) {
        n++;
      }
    }
  }

  if (n) {
    stream->stats_sacks += n;
  }
}

static void
fast_retransmit (udx_stream_t *stream) {
  udx_cirbuf_t *out = &(stream->outgoing);
  udx_packet_t *pkt = (udx_packet_t *) udx__cirbuf_get(out, stream->remote_acked);

  if (pkt == NULL || pkt->transmits != 1 || pkt->status != UDX_PACKET_INFLIGHT || pkt->is_retransmit) return;

  pkt->status = UDX_PACKET_WAITING;
  pkt->is_retransmit = 1;

  stream->inflight -= pkt->size;
  stream->pkts_waiting++;
  stream->pkts_inflight--;
  stream->retransmits_waiting++;
  stream->stats_fast_rt++;

  // Shrink the window
  stream->cwnd = max_uint32(UDX_MTU, stream->cwnd / 2);
}

static void
process_data_packet (udx_stream_t *stream, int type, uint32_t seq, char *data, ssize_t data_len) {
  if (seq == stream->ack && type == UDX_HEADER_DATA) {
    // Fast path - next in line, no need to memcpy it, stack allocate the struct and call on_read...
    stream->ack++;

    if (stream->on_read != NULL) {
      uv_buf_t buf = uv_buf_init(data, data_len);
      stream->on_read(stream, data_len, &buf);
    }
    return;
  }

  stream->out_of_order++;

  // Slow path, packet out of order.
  // Copy over incoming buffer as we do not own it (stack allocated upstream)
  char *ptr = malloc(sizeof(udx_pending_read_t) + data_len);

  udx_pending_read_t *pkt = (udx_pending_read_t *) ptr;
  char *cpy = ptr + sizeof(udx_pending_read_t);

  memcpy(cpy, data, data_len);

  pkt->type = type;
  pkt->seq = seq;
  pkt->buf.base = cpy;
  pkt->buf.len = data_len;

  stream->pkts_buffered++;
  udx__cirbuf_set(&(stream->incoming), (udx_cirbuf_val_t *) pkt);
}

static int
process_packet (udx_socket_t *socket, char *buf, ssize_t buf_len, struct sockaddr *addr) {
  if (buf_len < UDX_HEADER_SIZE) return 0;

  uint8_t *b = (uint8_t *) buf;

  if ((*(b++) != UDX_MAGIC_BYTE) || (*(b++) != UDX_VERSION)) return 0;

  int type = (int) *(b++);
  uint8_t data_offset = *(b++);

  uint32_t *i = (uint32_t *) b;

  uint32_t local_id = udx__swap_uint32_if_be(*(i++));
  /* recv_win */ udx__swap_uint32_if_be(*(i++));
  uint32_t seq = udx__swap_uint32_if_be(*(i++));
  uint32_t ack = udx__swap_uint32_if_be(*i);

  buf += UDX_HEADER_SIZE;
  buf_len -= UDX_HEADER_SIZE;

  udx_stream_t *stream = (udx_stream_t *) udx__cirbuf_get(socket->streams_by_id, local_id);

  if (stream == NULL || stream->status & UDX_STREAM_DEAD) return 0;

  // We expect this to be a stream packet from now on

  if (!(stream->status & UDX_STREAM_CONNECTED) && (stream->on_firewall != NULL && stream->on_firewall(stream, socket, addr))) {
    return 1;
  }

  udx_cirbuf_t *inc = &(stream->incoming);

  if (type & UDX_HEADER_SACK) {
    process_sacks(stream, buf, buf_len);
  }

  // Done with header processing now.
  // For future compat, make sure we are now pointing at the actual data using the data_offset
  if (data_offset) {
    if (data_offset > buf_len) return 1;
    buf += data_offset;
    buf_len -= data_offset;
  }

  // For all stream packets, ensure that they are causally newer (or same)
  if (seq_compare(stream->ack, seq) <= 0) {
    if (type & UDX_HEADER_DATA_OR_END && udx__cirbuf_get(inc, seq) == NULL && (stream->status & UDX_STREAM_SHOULD_READ) == UDX_STREAM_READ) {
      process_data_packet(stream, type, seq, buf, buf_len);
    }

    if (type & UDX_HEADER_END) {
      stream->status |= UDX_STREAM_ENDING_REMOTE;
      stream->remote_ended = seq;
    }

    if (type & UDX_HEADER_DESTROY) {
      stream->status |= UDX_STREAM_DESTROYED_REMOTE;
      clear_outgoing_packets(stream);
      close_maybe(stream, UV_ECONNRESET);
      return 1;
    }
  }

  if (type & UDX_HEADER_MESSAGE) {
    if (stream->on_recv != NULL) {
      uv_buf_t b = uv_buf_init(buf, buf_len);
      stream->on_recv(stream, buf_len, &b);
    }
  }

  // process the (out of order) read queue
  while ((stream->status & UDX_STREAM_SHOULD_READ) == UDX_STREAM_READ) {
    udx_pending_read_t *pkt = (udx_pending_read_t *) udx__cirbuf_remove(inc, stream->ack);
    if (pkt == NULL) break;

    stream->out_of_order--;
    stream->pkts_buffered--;
    stream->ack++;

    if ((pkt->type & UDX_HEADER_DATA) && stream->on_read != NULL) {
      stream->on_read(stream, pkt->buf.len, &(pkt->buf));
    }

    free(pkt);
  }

  // Check if the ack is oob.
  if (seq_compare(stream->seq, ack) < 0) {
    return 1;
  }

  // Congestion control...
  if (stream->remote_acked != ack) {
    if (stream->cwnd < stream->ssthresh) {
      stream->cwnd += UDX_MTU;
    } else {
      stream->cwnd += max_uint32((UDX_MTU * UDX_MTU) / stream->cwnd, 1);
    }
    stream->dup_acks = 0;
  } else if ((type & UDX_HEADER_DATA_OR_END) == 0) {
    stream->dup_acks++;
    if (stream->dup_acks >= 3) {
      fast_retransmit(stream);
    }
  }

  int32_t len = seq_diff(ack, stream->remote_acked);

  for (int32_t j = 0; j < len; j++) {
    int a = ack_packet(stream, stream->remote_acked++, 0);
    if (a == 0 || a == 1) continue;
    if (a == 2) { // it ended, so ack that and trigger close
      // TODO: make this work as well, if the ack packet is lost, ie
      // have some internal (capped) queue of "gracefully closed" streams (TIME_WAIT)
      send_state_packet(stream);
      close_maybe(stream, 0);
    }
    return 1;
  }

  // if data pkt, send an ack - use deferred acks as well...
  if (type & UDX_HEADER_DATA_OR_END) {
    send_state_packet(stream);
  }

  if ((stream->status & UDX_STREAM_SHOULD_END_REMOTE) == UDX_STREAM_END_REMOTE && seq_compare(stream->remote_ended, stream->ack) <= 0) {
    stream->status |= UDX_STREAM_ENDED_REMOTE;
    if (stream->on_read != NULL) {
      uv_buf_t b = uv_buf_init(NULL, 0);
      stream->on_read(stream, UV_EOF, &b);
    }
    if (close_maybe(stream, 0)) return 1;
  }

  if (stream->pkts_waiting > 0) {
    udx_stream_check_timeouts(stream);
  }

  return 1;
}

static void
trigger_send_callback (udx_socket_t *socket, udx_packet_t *pkt) {
  if (pkt->type == UDX_PACKET_SEND) {
    udx_socket_send_t *req = pkt->ctx;

    if (req->on_send != NULL) {
      req->on_send(req, 0);
    }
    return;
  }

  if (pkt->type == UDX_PACKET_STREAM_SEND) {
    udx_stream_send_t *req = pkt->ctx;

    if (req->on_send != NULL) {
      req->on_send(req, 0);
    }
    return;
  }

  if (pkt->type == UDX_PACKET_STREAM_DESTROY) {
    udx_stream_t *stream = pkt->ctx;

    stream->status |= UDX_STREAM_DESTROYED;
    close_maybe(stream, 0);
    return;
  }
}

static void
on_uv_poll (uv_poll_t *handle, int status, int events) {
  udx_socket_t *socket = handle->data;

  if (socket->send_queue.len > 0 && events & UV_WRITABLE) {
    udx_packet_t *pkt = (udx_packet_t *) udx__fifo_shift(&(socket->send_queue));

    if (pkt == NULL) return;

    assert(pkt->status == UDX_PACKET_SENDING);
    pkt->status = UDX_PACKET_INFLIGHT;
    pkt->transmits++;

    bool adjust_ttl = pkt->ttl > 0 && socket->ttl != pkt->ttl;

    if (adjust_ttl) uv_udp_set_ttl((uv_udp_t *) socket, pkt->ttl);

    udx__sendmsg(socket, pkt->bufs, pkt->bufs_len, &(pkt->dest), sizeof(pkt->dest));

    pkt->time_sent = uv_hrtime() / 1e6;

    if (adjust_ttl) uv_udp_set_ttl((uv_udp_t *) socket, socket->ttl);

    int type = pkt->type;

    if (type & UDX_PACKET_CALLBACK) {
      trigger_send_callback(socket, pkt);
      // TODO: watch for re-entry here!
    }

    if (type & UDX_PACKET_FREE_ON_SEND) {
      free(pkt);
    }

    // queue another write, might be able to do this smarter...
    if (socket->send_queue.len > 0) {
      return;
    }

    // if the socket is under closure, we need to trigger shutdown now since no important writes are pending
    if (socket->status & UDX_SOCKET_CLOSING) {
      close_handles(socket);
      return;
    }
  }

  if (events & UV_READABLE) {
    struct sockaddr_storage addr;
    int addr_len = sizeof(addr);
    uv_buf_t buf;

    memset(&addr, 0, addr_len);

    char b[2048];
    buf.base = (char *) &b;
    buf.len = 2048;

    ssize_t size = udx__recvmsg(socket, &buf, (struct sockaddr *) &addr, addr_len);

    if (size >= 0 && !process_packet(socket, b, size, (struct sockaddr *) &addr) && socket->on_recv != NULL) {
      buf.len = size;
      socket->on_recv(socket, size, &buf, (struct sockaddr *) &addr);
    }

    return;
  }

  // update the poll if the socket is still active.
  if (uv_is_active((uv_handle_t *) &socket->io_poll)) {
    update_poll(socket);
  }
}

int
udx_init (uv_loop_t *loop, udx_t *handle) {
  handle->refs = 0;
  handle->sockets = 0;
  handle->timer_closed_by = NULL;

  handle->streams_len = 0;
  handle->streams_max_len = 0;
  handle->streams = NULL;

  handle->loop = loop;

  return 0;
}

int
udx_check_timeouts (udx_t *handle) {
  for (uint32_t i = 0; i < handle->streams_len; i++) {
    int err = udx_stream_check_timeouts(handle->streams[i]);
    if (err < 0) return err;
    if (err == 1) i--; // stream was closed, the index again
  }
  return 0;
}

int
udx_socket_init (udx_t *udx, udx_socket_t *handle) {
  ref_inc(udx);

  handle->status = 0;
  handle->events = 0;
  handle->pending_closes = 0;
  handle->ttl = UDX_DEFAULT_TTL;

  handle->udx = udx;
  handle->streams_by_id = &(udx->streams_by_id);

  udx->sockets++;

  // If first open...
  if (udx->sockets == 1 && udx->timer_closed_by == NULL) {
    // Asserting all the errors here as it massively simplifies error handling.
    // In practice these will never fail.
    udx_start_timer(udx);
  }

  handle->on_recv = NULL;
  handle->on_close = NULL;

  udx__fifo_init(&(handle->send_queue), 16);

  uv_udp_t *socket = &(handle->socket);

  // Asserting all the errors here as it massively simplifies error handling.
  // In practice these will never fail.

  int err = uv_udp_init(udx->loop, socket);
  assert(err == 0);

  socket->data = handle;

  return err;
}

int
udx_socket_send_buffer_size(udx_socket_t *handle, int *value) {
  return uv_send_buffer_size((uv_handle_t *) &(handle->socket), value);
}

int
udx_socket_recv_buffer_size(udx_socket_t *handle, int *value) {
  return uv_recv_buffer_size((uv_handle_t *) &(handle->socket), value);
}

int
udx_socket_set_ttl(udx_socket_t *handle, int ttl) {
  if (ttl < 1 || ttl > 255) return UV_EINVAL;
  handle->ttl = ttl;
  return uv_udp_set_ttl((uv_udp_t *) &(handle->socket), ttl);
}

int
udx_socket_bind (udx_socket_t *handle, const struct sockaddr *addr) {
  uv_udp_t *socket = &(handle->socket);
  uv_poll_t *poll = &(handle->io_poll);
  uv_os_fd_t fd;

  // This might actually fail in practice, so
  int err = uv_udp_bind(socket, addr, 0);
  if (err) return err;

  // Asserting all the errors here as it massively simplifies error handling
  // and in practice non of these will fail, as all our handles are valid and alive.

  err = uv_udp_set_ttl(socket, handle->ttl);
  assert(err == 0);

  err = uv_fileno((const uv_handle_t *) socket, &fd);
  assert(err == 0);

  err = uv_poll_init_socket(handle->udx->loop, poll, fd);
  assert(err == 0);

  handle->status |= UDX_SOCKET_BOUND;
  poll->data = handle;

  return update_poll(handle);
}

int
udx_socket_getsockname (udx_socket_t *handle, struct sockaddr *name, int *name_len) {
  return uv_udp_getsockname(&(handle->socket), name, name_len);
}

int
udx_socket_send (udx_socket_send_t *req, udx_socket_t *handle, const uv_buf_t bufs[], unsigned int bufs_len, const struct sockaddr *dest, udx_socket_send_cb cb) {
  return udx_socket_send_ttl(req, handle, bufs, bufs_len, dest, 0, cb);
}

int
udx_socket_send_ttl (udx_socket_send_t *req, udx_socket_t *handle, const uv_buf_t bufs[], unsigned int bufs_len, const struct sockaddr *dest, int ttl, udx_socket_send_cb cb) {
  if (ttl < 0 /* 0 is "default" */ || ttl > 255) return UV_EINVAL;

  assert(bufs_len == 1);

  req->handle = handle;
  req->on_send = cb;

  udx_packet_t *pkt = &(req->pkt);

  pkt->status = UDX_PACKET_SENDING;
  pkt->type = UDX_PACKET_SEND;
  pkt->ttl = ttl;
  pkt->ctx = req;
  pkt->dest = *dest;

  pkt->is_retransmit = 0;
  pkt->transmits = 0;

  pkt->bufs_len = 1;

  pkt->bufs[0] = bufs[0];

  pkt->fifo_gc = udx__fifo_push(&(handle->send_queue), pkt);

  return update_poll(handle);
}

int
udx_socket_recv_start (udx_socket_t *handle, udx_socket_recv_cb cb) {
  if (handle->status & UDX_SOCKET_RECEIVING) return UV_EALREADY;

  handle->on_recv = cb;
  handle->status |= UDX_SOCKET_RECEIVING;

  return update_poll(handle);
}

int
udx_socket_recv_stop (udx_socket_t *handle) {
  if ((handle->status & UDX_SOCKET_RECEIVING) == 0) return 0;

  handle->on_recv = NULL;
  handle->status ^= UDX_SOCKET_RECEIVING;

  return update_poll(handle);
}

int
udx_socket_close (udx_socket_t *handle, udx_socket_close_cb cb) {
  // if (handle->streams_len > 0) return UV_EBUSY;

  handle->status |= UDX_SOCKET_CLOSING;

  handle->on_close = cb;

  // allow stream packets to flush, but cancel anything else
  int queued = handle->send_queue.len;

  while (queued--) {
    udx_packet_t *pkt = udx__fifo_shift(&(handle->send_queue));
    if (pkt == NULL) break;

    if (pkt->type == UDX_PACKET_SEND) {
      udx_socket_send_t *req = pkt->ctx;

      if (req->on_send != NULL) {
        req->on_send(req, UV_ECANCELED);
      }

      continue;
    }

    // stream packet, allow them to flush, by requeueing them
    // flips the order but these are all state packets so whatevs
    udx__fifo_push(&(handle->send_queue), pkt);
  }

  if (handle->send_queue.len == 0) {
    close_handles(handle);
  }

  return 0;
}

int
udx_stream_init (udx_t *udx, udx_stream_t *handle, uint32_t local_id) {
  ref_inc(udx);

  handle->local_id = local_id;
  handle->remote_id = 0;
  handle->set_id = 0;
  handle->status = 0;
  handle->out_of_order = 0;
  handle->socket = NULL;
  handle->udx = udx;

  handle->seq = 0;
  handle->ack = 0;
  handle->remote_acked = 0;

  handle->srtt = 0;
  handle->rttvar = 0;
  handle->rto = 1000;
  handle->rto_timeout = get_milliseconds() + handle->rto;

  handle->pkts_waiting = 0;
  handle->pkts_inflight = 0;
  handle->pkts_buffered = 0;
  handle->dup_acks = 0;
  handle->retransmits_waiting = 0;

  handle->inflight = 0;
  handle->ssthresh = 0xffff;
  handle->cwnd = 2 * UDX_MTU;
  handle->rwnd = 0;

  handle->stats_sacks = 0;
  handle->stats_pkts_sent = 0;
  handle->stats_fast_rt = 0;

  handle->on_firewall = NULL;
  handle->on_read = NULL;
  handle->on_recv = NULL;
  handle->on_drain = NULL;
  handle->on_close = NULL;

  // Init stream write/read buffers
  udx__cirbuf_init(&(handle->outgoing), 16);
  udx__cirbuf_init(&(handle->incoming), 16);

  handle->set_id = udx->streams_len++;

  if (udx->streams_len == udx->streams_max_len) {
    udx->streams_max_len *= 2;
    udx->streams = realloc(udx->streams, udx->streams_max_len * sizeof(udx_stream_t *));
  }

  udx->streams[handle->set_id] = handle;

  // Add the socket to the active set

  udx__cirbuf_set(&(udx->streams_by_id), (udx_cirbuf_val_t *) handle);

  return 0;
}

int
udx_stream_firewall (udx_stream_t *handle, udx_stream_firewall_cb cb) {
  handle->on_firewall = cb;

  return 0;
}

int
udx_stream_recv_start (udx_stream_t *handle, udx_stream_recv_cb cb) {
  if (handle->status & UDX_STREAM_RECEIVING) return UV_EALREADY;

  handle->on_recv = cb;
  handle->status |= UDX_STREAM_RECEIVING;

  return handle->socket == NULL ? 0 : update_poll(handle->socket);
}

int
udx_stream_recv_stop (udx_stream_t *handle) {
  if ((handle->status & UDX_STREAM_RECEIVING) == 0) return 0;

  handle->on_recv = NULL;
  handle->status ^= UDX_STREAM_RECEIVING;

  return handle->socket == NULL ? 0 : update_poll(handle->socket);
}

int
udx_stream_read_start (udx_stream_t *handle, udx_stream_read_cb cb) {
  if (handle->status & UDX_STREAM_READING) return UV_EALREADY;

  handle->on_read = cb;
  handle->status |= UDX_STREAM_READING;

  return handle->socket == NULL ? 0 : update_poll(handle->socket);
}

int
udx_stream_read_stop (udx_stream_t *handle) {
  if ((handle->status & UDX_STREAM_READING) == 0) return 0;

  handle->on_read = NULL;
  handle->status ^= UDX_STREAM_READING;

  return handle->socket == NULL ? 0 : update_poll(handle->socket);
}

int
udx_stream_check_timeouts (udx_stream_t *handle) {
  if (handle->remote_acked == handle->seq || (handle->status & UDX_STREAM_CONNECTED) == 0) return 0;

  const uint64_t now = handle->inflight ? get_milliseconds() : 0;

  if (now > handle->rto_timeout) {
    // Ensure it backs off until data is acked...
    handle->rto_timeout = now + 2 * handle->rto;

    // Consider all packet losts - seems to be the simple consensus across different stream impls
    // which we like cause it is nice and simple to implement.
    for (uint32_t seq = handle->remote_acked; seq != handle->seq; seq++) {
      udx_packet_t *pkt = (udx_packet_t *) udx__cirbuf_get(&(handle->outgoing), seq);

      if (pkt == NULL || pkt->status != UDX_PACKET_INFLIGHT || pkt->is_retransmit) continue;

      if (pkt->transmits >= UDX_MAX_TRANSMITS) {
        handle->status |= UDX_STREAM_DESTROYED;
        close_maybe(handle, UV_ETIMEDOUT);
        return 1;
      }

      pkt->status = UDX_PACKET_WAITING;
      pkt->is_retransmit = 1;

      handle->inflight -= pkt->size;
      handle->pkts_waiting++;
      handle->pkts_inflight--;
      handle->retransmits_waiting++;
    }

    handle->cwnd = max_uint32(UDX_MTU, handle->cwnd / 2);

    debug_printf("pkt loss! stream is congested, scaling back (requeued the full window)\n");
  }

  int err = flush_waiting_packets(handle);
  return err < 0 ? err : 0;
}

int
udx_stream_connect (udx_stream_t *handle, udx_socket_t *socket, uint32_t remote_id, const struct sockaddr *remote_addr, udx_stream_close_cb close_cb) {
  if (handle->status & UDX_STREAM_CONNECTED) {
    return UV_EISCONN;
  }

  handle->status |= UDX_STREAM_CONNECTED;

  handle->remote_id = remote_id;
  handle->remote_addr = *remote_addr;
  handle->socket = socket;

  handle->on_close = close_cb;

  return update_poll(handle->socket);
}

int
udx_stream_send (udx_stream_send_t *req, udx_stream_t *handle, const uv_buf_t bufs[], unsigned int bufs_len, udx_stream_send_cb cb) {
  assert(bufs_len == 1);

  req->handle = handle;
  req->on_send = cb;

  udx_socket_t *socket = handle->socket;
  udx_packet_t *pkt = &(req->pkt);

  init_stream_packet(pkt, UDX_HEADER_MESSAGE, handle, &bufs[0]);

  pkt->status = UDX_PACKET_SENDING;
  pkt->type = UDX_PACKET_STREAM_SEND;
  pkt->ttl = 0;
  pkt->ctx = req;
  pkt->is_retransmit = 0;
  pkt->transmits = 0;

  pkt->fifo_gc = udx__fifo_push(&(socket->send_queue), pkt);
  return update_poll(socket);
}

int
udx_stream_write_resume (udx_stream_t *handle, udx_stream_drain_cb drain_cb) {
  handle->on_drain = drain_cb;
  return 0;
}

int
udx_stream_write (udx_stream_write_t *req, udx_stream_t *handle, const uv_buf_t bufs[], unsigned int bufs_len, udx_stream_ack_cb ack_cb) {
  assert(bufs_len == 1);

  req->packets = 0;
  req->handle = handle;
  req->on_ack = ack_cb;

  // if this is the first inflight packet, we should "restart" rto timer
  if (handle->socket != NULL && handle->inflight == 0) {
    handle->rto_timeout = get_milliseconds() + handle->rto;
  }

  int err = 0;

  uv_buf_t buf = bufs[0];

  do {
    udx_packet_t *pkt = malloc(sizeof(udx_packet_t));

    size_t buf_partial_len = buf.len < UDX_MAX_DATA_SIZE ? buf.len : UDX_MAX_DATA_SIZE;
    uv_buf_t buf_partial = uv_buf_init(buf.base, buf_partial_len);

    init_stream_packet(pkt, UDX_HEADER_DATA, handle, &buf_partial);

    pkt->status = UDX_PACKET_WAITING;
    pkt->type = UDX_PACKET_STREAM_WRITE;
    pkt->ttl = 0;
    pkt->ctx = req;

    handle->seq++;
    req->packets++;

    buf.len -= buf_partial_len;
    buf.base += buf_partial_len;

    udx__cirbuf_set(&(handle->outgoing), (udx_cirbuf_val_t *) pkt);

    // If we are not the first packet in the queue, wait to send us until the queue is flushed...
    if (handle->pkts_waiting++ > 0) continue;

    if (handle->socket != NULL) err = send_data_packet(handle, pkt);
  } while (buf.len > 0 || err < 0);

  return err;
}

int
udx_stream_write_end (udx_stream_write_t *req, udx_stream_t *handle, const uv_buf_t bufs[], unsigned int bufs_len, udx_stream_ack_cb ack_cb) {
  assert(bufs_len == 1);

  handle->status |= UDX_STREAM_ENDING;

  req->packets = 0;
  req->handle = handle;
  req->on_ack = ack_cb;

  int err = 0;

  uv_buf_t buf = bufs[0];

  do {
    udx_packet_t *pkt = malloc(sizeof(udx_packet_t));

    size_t buf_partial_len = buf.len < UDX_MAX_DATA_SIZE ? buf.len : UDX_MAX_DATA_SIZE;
    uv_buf_t buf_partial = uv_buf_init(buf.base, buf_partial_len);

    init_stream_packet(pkt, UDX_HEADER_END, handle, &buf_partial);

    pkt->status = UDX_PACKET_WAITING;
    pkt->type = UDX_PACKET_STREAM_WRITE;
    pkt->ttl = 0;
    pkt->ctx = req;

    handle->seq++;
    req->packets++;

    buf.len -= buf_partial_len;
    buf.base += buf_partial_len;

    udx__cirbuf_set(&(handle->outgoing), (udx_cirbuf_val_t *) pkt);

    // If we are not the first packet in the queue, wait to send us until the queue is flushed...
    if (handle->pkts_waiting++ > 0) continue;

    if (handle->socket != NULL) err = send_data_packet(handle, pkt);
  } while (buf.len > 0 || err < 0);

  return err;
}

int
udx_stream_destroy (udx_stream_t *handle) {
  if ((handle->status & UDX_STREAM_CONNECTED) == 0) {
    handle->status |= UDX_STREAM_DESTROYED;
    close_maybe(handle, 0);
    return 0;
  }

  handle->status |= UDX_STREAM_DESTROYING;

  clear_outgoing_packets(handle);

  udx_packet_t *pkt = malloc(sizeof(udx_packet_t));

  uv_buf_t buf = uv_buf_init(NULL, 0);

  init_stream_packet(pkt, UDX_HEADER_DESTROY, handle, &buf);

  pkt->status = UDX_PACKET_SENDING;
  pkt->type = UDX_PACKET_STREAM_DESTROY;
  pkt->ttl = 0;
  pkt->ctx = handle;

  handle->seq++;

  udx__fifo_push(&(handle->socket->send_queue), pkt);

  int err = update_poll(handle->socket);
  return err < 0 ? err : 1;
}
