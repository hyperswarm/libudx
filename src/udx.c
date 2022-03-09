#include <assert.h>
#include <string.h>

#include "../include/udx.h"

#include "cirbuf.h"
#include "fifo.h"
#include "io.h"

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

#define UDX_MAX_TRANSMITS 5

typedef struct {
  uint32_t seq; // must be the first entry, so its compat with the cirbuf

  int type;

  uv_buf_t buf;
} udx_pending_read_t;

static uint32_t
random_id () {
  uint32_t id;
  uv_random(NULL, NULL, &id, sizeof(id), 0, NULL);
  return id;
}

static uint64_t
get_microseconds () {
  return uv_hrtime() / 1000;
}

static uint64_t
get_milliseconds () {
  return get_microseconds() / 1000;
}

static uint32_t
max (a, b) {
  return a < b ? b : a;
}

static void
on_uv_poll (uv_poll_t *handle, int status, int events);

static void
on_uv_close (uv_handle_t *handle) {
  udx_t *socket = (udx_t *) handle->data;

  if (--socket->pending_closes == 0 && socket->on_close != NULL) {
    socket->on_close(socket);
  }
}

static void
on_uv_interval (uv_timer_t *handle) {
  udx_t *socket = handle->data;
  udx_check_timeouts(socket);
}

static int
update_poll (udx_t *socket) {
  int events = (socket->send_queue.len > 0 ? UV_WRITABLE : 0) | (socket->readers ? UV_READABLE : 0);
  if (events == socket->events) return 0;

  socket->events = events;
  return uv_poll_start(&(socket->io_poll), events, on_uv_poll);
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

  // TODO: the header is ALWAYS little endian, make this work on big endian archs also

  // 32 bit (le) remote id
  *(i++) = stream->remote_id;
  // 32 bit (le) recv window
  *(i++) = 0xffffffff; // hardcode max recv window
  // 32 bit (le) seq
  *(i++) = pkt->seq = stream->seq;
  // 32 bit (le) ack
  *(i++) = stream->ack;

  pkt->transmits = 0;
  pkt->size = (uint16_t) (UDX_HEADER_SIZE + buf->len);
  pkt->dest = stream->remote_addr;

  pkt->bufs_len = 2;

  pkt->bufs[0] = uv_buf_init((char *) &(pkt->header), UDX_HEADER_SIZE);
  pkt->bufs[1] = *buf;
}

static int
send_state_packet (udx_stream_t *stream) {
  uint32_t *sacks = NULL;
  uint32_t start = 0;
  uint32_t end = 0;

  udx_packet_t *pkt = NULL;

  void *payload = NULL;
  size_t payload_len = 0;

  uint32_t max = 512;
  for (uint32_t i = 0; i < max && payload_len < 400; i++) {
    uint32_t seq = stream->ack + 1 + i;
    if (udx__cirbuf_get(&(stream->incoming), seq) == NULL) continue;

    if (sacks == NULL) {
      pkt = malloc(sizeof(udx_packet_t) + 1024);
      payload = (((void *) pkt) + sizeof(udx_packet_t));
      sacks = (uint32_t *) payload;
      start = seq;
      end = seq + 1;
    } else if (seq == end) {
      end++;
    } else {
      *(sacks++) = start;
      *(sacks++) = end;
      start = seq;
      end = seq + 1;
      payload_len += 8;
    }

    max = i + 512;
  }

  if (start != end) {
    *(sacks++) = start;
    *(sacks++) = end;
    payload_len += 8;
  }

  if (pkt == NULL) pkt = malloc(sizeof(udx_packet_t));

  uv_buf_t buf = uv_buf_init(payload, payload_len);

  init_stream_packet(pkt, payload ? UDX_HEADER_SACK : 0, stream, &buf);

  pkt->status = UDX_PACKET_SENDING;
  pkt->type = UDX_PACKET_STREAM_STATE;

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
  if (pkt->transmits > 0) stream->retransmits_waiting--;

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

  udx_t *socket = stream->socket;
  socket->streams[stream->set_id] = socket->streams[--(socket->streams_len)];

  // TODO: Dealloc all remaning state such as
  // - pending reads
  // - destroy alloc'ed cirbufs
  // (anything else from stream init)

  udx__cirbuf_remove(&(stream->socket->streams_by_id), stream->local_id);
  // TODO: move the instance to a TIME_WAIT state, so we can handle retransmits

  if (stream->status & UDX_STREAM_CONNECTED) {
    // TODO: move this to read_stop
    stream->socket->readers--;
    update_poll(stream->socket);
  }

  if (stream->on_close != NULL) {
    stream->on_close(stream, err);
  }

  return 1;
}

static int
ack_packet (udx_stream_t *stream, uint32_t seq, int sack) {
  udx_cirbuf_t *out = &(stream->outgoing);
  udx_packet_t *pkt = (udx_packet_t *) udx__cirbuf_remove(out, seq);

  if (pkt == NULL) return 0;

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
      stream->rto = stream->srtt + max(UDX_CLOCK_GRANULARITY_MS, 4 * stream->rttvar);
    } else {
      const uint32_t delta = rtt < stream->srtt ? stream->srtt - rtt : rtt - stream->srtt;
      // RTTVAR <- (1 - beta) * RTTVAR + beta * |SRTT - R'| where beta is 1/4
      stream->rttvar = (3 * stream->rttvar + delta) / 4;

      // SRTT <- (1 - alpha) * SRTT + alpha * R' where alpha is 1/8
      stream->srtt = (7 * stream->srtt + rtt) / 8;
    }

    // RTO <- SRTT + max (G, K*RTTVAR) where K is 4 maxed with 1s
    stream->rto = max(stream->srtt + max(UDX_CLOCK_GRANULARITY_MS, 4 * stream->rttvar), 1000);
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
    uint32_t start = *(sacks++);
    uint32_t end = *(sacks++);
    uint32_t len = end - start;

    for (uint32_t j = 0; j < len; j++) {
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

  if (pkt == NULL || pkt->transmits != 1 || pkt->status != UDX_PACKET_INFLIGHT) return;

  pkt->status = UDX_PACKET_WAITING;

  stream->inflight -= pkt->size;
  stream->pkts_waiting++;
  stream->pkts_inflight--;
  stream->retransmits_waiting++;
  stream->stats_fast_rt++;

  // Shrink the window
  stream->cwnd = max(UDX_MTU, stream->cwnd / 2);
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

    if (--(w->packets) == 0 && w->on_ack != NULL) {
      w->on_ack(w, 1, 0);
    }

    free(pkt);
  }
}

static int
process_packet (udx_t *socket, char *buf, ssize_t buf_len) {
  if (buf_len < UDX_HEADER_SIZE) return 0;

  uint8_t *b = (uint8_t *) buf;

  if ((*(b++) != UDX_MAGIC_BYTE) || (*(b++) != UDX_VERSION)) return 0;

  int type = (int) *(b++);
  uint8_t data_offset = *(b++);

  uint32_t *i = (uint32_t *) b;

  uint32_t local_id = *(i++);
  uint32_t recv_win = *(i++);
  uint32_t seq = *(i++);
  uint32_t ack = *i;

  buf += UDX_HEADER_SIZE;
  buf_len -= UDX_HEADER_SIZE;

  udx_stream_t *stream = (udx_stream_t *) udx__cirbuf_get(&(socket->streams_by_id), local_id);
  if (stream == NULL || stream->status & UDX_STREAM_DEAD) return 0;

  udx_cirbuf_t *inc = &(stream->incoming);

  if (type & UDX_HEADER_SACK) {
    process_sacks(stream, buf, buf_len);
  }

  if (type & UDX_HEADER_DATA_OR_END && udx__cirbuf_get(inc, seq) == NULL && (stream->status & UDX_STREAM_SHOULD_READ) == UDX_STREAM_READ) {
    // Copy over incoming buffer as we CURRENTLY do not own it (stack allocated upstream)
    // TODO: if this is the next packet we expect (it usually is!), then there is no need
    // for the malloc and memcpy - we just need a way to not free it then

    if (data_offset) {
      if (data_offset > buf_len) return 0;
      buf += data_offset;
      buf_len -= data_offset;
    }

    char *ptr = malloc(sizeof(udx_pending_read_t) + buf_len);

    udx_pending_read_t *pkt = (udx_pending_read_t *) ptr;
    char *cpy = ptr + sizeof(udx_pending_read_t);

    memcpy(cpy, buf, buf_len);

    pkt->seq = seq;
    pkt->buf.base = cpy;
    pkt->buf.len = buf_len;

    udx__cirbuf_set(inc, (udx_cirbuf_val_t *) pkt);
  }

  if (type & UDX_HEADER_END) {
    stream->status |= UDX_STREAM_ENDING_REMOTE;
    stream->remote_ended = seq;
  }

  if (type & UDX_HEADER_MESSAGE) {
    if (data_offset) {
      if (data_offset > buf_len) return 0;
      buf += data_offset;
      buf_len -= data_offset;
    }

    if (stream->on_recv != NULL) {
      uv_buf_t b = uv_buf_init(buf, buf_len);
      stream->on_recv(stream, buf_len, &b);
    }
  }

  if (type & UDX_HEADER_DESTROY) {
    stream->status |= UDX_STREAM_DESTROYED_REMOTE;
    clear_outgoing_packets(stream);
    close_maybe(stream, UDX_ERROR_DESTROYED_REMOTE);
    return 1;
  }

  // process the read queue
  while ((stream->status & UDX_STREAM_SHOULD_READ) == UDX_STREAM_READ) {
    udx_pending_read_t *pkt = (udx_pending_read_t *) udx__cirbuf_remove(inc, stream->ack);
    if (pkt == NULL) break;

    stream->ack++;

    if (pkt->buf.len > 0 && stream->on_read != NULL) {
      uv_buf_t b = uv_buf_init(pkt->buf.base, pkt->buf.len);
      stream->on_read(stream, pkt->buf.len, &b);
    }

    free(pkt);
  }

  // Check if the ack is oob.
  if (stream->seq < ack) {
    return 1;
  }

  // Congestion control...
  if (stream->remote_acked != ack) {
    if (stream->cwnd < stream->ssthresh) {
      stream->cwnd += UDX_MTU;
    } else {
      stream->cwnd += max((UDX_MTU * UDX_MTU) / stream->cwnd, 1);
    }
    stream->dup_acks = 0;
  } else if ((type & UDX_HEADER_DATA_OR_END) == 0) {
    stream->dup_acks++;
    if (stream->dup_acks >= 3) {
      fast_retransmit(stream);
    }
  }

  while (stream->remote_acked < ack) {
    int a = ack_packet(stream, stream->remote_acked++, 0);
    if (a == 1) continue;
    if (a == 2) { // it ended, so ack that and trigger close
      // TODO: make this work as well, if the ack packet is lost, ie
      // have some internal (capped) queue of "gracefully closed" streams
      send_state_packet(stream);
      close_maybe(stream, 0);
    }
    return 1;
  }

  // if data pkt, send an ack - use deferred acks as well...
  if (type & UDX_HEADER_DATA_OR_END) {
    send_state_packet(stream);
  }

  if ((stream->status & UDX_STREAM_SHOULD_END_REMOTE) == UDX_STREAM_END_REMOTE && stream->remote_ended <= stream->ack) {
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
trigger_send_callback (udx_t *socket, udx_packet_t *pkt) {
  if (pkt->type == UDX_PACKET_SEND) {
    udx_send_t *req = pkt->ctx;

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
    close_maybe(stream, UDX_ERROR_DESTROYED);
    return;
  }
}

static void
on_uv_poll (uv_poll_t *handle, int status, int events) {
  udx_t *socket = handle->data;

  if (socket->send_queue.len > 0 && events & UV_WRITABLE) {
    udx_packet_t *pkt = (udx_packet_t *) udx__fifo_shift(&(socket->send_queue));

    if (pkt == NULL) return;

    assert(pkt->status == UDX_PACKET_SENDING);
    pkt->status = UDX_PACKET_INFLIGHT;
    pkt->transmits++;

    udx__sendmsg(socket, pkt);

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
  }

  if (events & UV_READABLE) {
    struct sockaddr addr;
    uv_buf_t buf;

    char b[2048];
    buf.base = (char *) &b;
    buf.len = 2048;

    ssize_t size = udx__recvmsg(socket, &buf, &addr);

    if (size > 0 && !process_packet(socket, b, size) && socket->on_recv != NULL) {
      buf.len = size;
      socket->on_recv(socket, size, &buf, &addr);
    }

    return;
  }

  update_poll(socket);
}

int
udx_init (uv_loop_t *loop, udx_t *handle) {
  handle->status = 0;
  handle->readers = 0;
  handle->events = 0;

  handle->streams_len = 0;
  handle->streams_max_len = 16;
  handle->streams = malloc(handle->streams_max_len * sizeof(udx_stream_t *));

  handle->loop = loop;

  handle->on_recv = NULL;
  handle->on_close = NULL;

  udx__fifo_init(&(handle->send_queue), 16);
  udx__cirbuf_init(&(handle->streams_by_id), 1);

  uv_udp_t *socket = &(handle->socket);
  uv_timer_t *timer = &(handle->timer);

  // Asserting all the errors here as it massively simplifies error handling.
  // In practice these will never fail.

  int err = uv_timer_init(loop, timer);
  assert(err == 0);

  err = uv_udp_init(loop, socket);
  assert(err == 0);

  timer->data = handle;
  socket->data = handle;

  return 0;
}

int
udx_send_buffer_size(udx_t *handle, int *value) {
  return uv_send_buffer_size((uv_handle_t *) &(handle->socket), value);
}

int
udx_recv_buffer_size(udx_t *handle, int *value) {
  return uv_recv_buffer_size((uv_handle_t *) &(handle->socket), value);
}

int
udx_set_ttl(udx_t *handle, int ttl) {
  return uv_udp_set_ttl((uv_udp_t *) &(handle->socket), ttl);
}

int
udx_bind (udx_t *handle, const struct sockaddr *addr) {
  uv_udp_t *socket = &(handle->socket);
  uv_poll_t *poll = &(handle->io_poll);
  uv_os_fd_t fd;

  // This might actually fail in practice, so
  int err = uv_udp_bind(socket, addr, 0);
  if (err) return err;

  // Asserting all the errors here as it massively simplifies error handling
  // and in practice non of these will fail, as all our handles are valid and alive.

  err = uv_fileno((const uv_handle_t *) socket, &fd);
  assert(err == 0);

  err = uv_poll_init(handle->loop, poll, fd);
  assert(err == 0);

  err = uv_timer_start(&(handle->timer), on_uv_interval, UDX_CLOCK_GRANULARITY_MS, UDX_CLOCK_GRANULARITY_MS);
  assert(err == 0);

  handle->status |= UDX_SOCKET_BOUND;
  poll->data = handle;

  return 0;
}

int
udx_getsockname (udx_t *handle, struct sockaddr * name, int *name_len) {
  return uv_udp_getsockname(&(handle->socket), name, name_len);
}

int
udx_send (udx_send_t *req, udx_t *handle, const uv_buf_t bufs[], unsigned int bufs_len, const struct sockaddr *dest, udx_send_cb cb) {
  assert(bufs_len == 1);

  req->handle = handle;
  req->on_send = cb;

  udx_packet_t *pkt = &(req->pkt);

  pkt->status = UDX_PACKET_SENDING;
  pkt->type = UDX_PACKET_SEND;
  pkt->ctx = req;
  pkt->dest = *dest;

  pkt->transmits = 0;

  pkt->bufs_len = 1;

  pkt->bufs[0] = bufs[0];

  pkt->fifo_gc = udx__fifo_push(&(handle->send_queue), pkt);

  return update_poll(handle);
}

int
udx_recv_start (udx_t *handle, udx_recv_cb cb) {
  if (handle->status & UDX_SOCKET_READING) return 0;

  handle->on_recv = cb;
  handle->status |= UDX_SOCKET_READING;
  handle->readers++;

  return update_poll(handle);
}

int
udx_recv_stop (udx_t *handle) {
  if ((handle->status & UDX_SOCKET_READING) == 0) return 0;

  handle->on_recv = NULL;
  handle->status ^= UDX_SOCKET_PAUSED;
  handle->readers--;

  return update_poll(handle);
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
udx_close (udx_t *handle, udx_close_cb cb) {
  if (handle->streams_len > 0) return UV_EBUSY;

  handle->on_close = cb;
  handle->pending_closes = 2;
  uv_timer_stop(&(handle->timer));

  if (handle->status & UDX_SOCKET_BOUND) {
    handle->pending_closes++;
    uv_poll_stop(&(handle->io_poll));
    uv_close((uv_handle_t *) &(handle->io_poll), on_uv_close);
  }

  uv_close((uv_handle_t *) &(handle->socket), on_uv_close);
  uv_close((uv_handle_t *) &(handle->timer), on_uv_close);

  while (1) {
    udx_packet_t *pkt = udx__fifo_shift(&handle->send_queue);
    if (pkt == NULL) break;

    if (pkt->type == UDX_PACKET_SEND) {
      udx_send_t *req = pkt->ctx;

      if (req->on_send != NULL) {
        req->on_send(req, UDX_ERROR_DESTROYED);
      }
    }
  }

  return 0;
}

int
udx_stream_init (udx_t *socket, udx_stream_t *handle, uint32_t *local_id, udx_stream_close_cb close_cb) {
  if (socket->streams_len >= 65536) return -1;

  // Get a free socket id (pick a random one until we get a free one)
  uint32_t id;
  while (1) {
    id = random_id();
    udx_cirbuf_val_t *v = udx__cirbuf_get(&(socket->streams_by_id), id);
    if (v == NULL) break;
  }

  if (socket->streams_len == socket->streams_max_len) {
    socket->streams_max_len *= 2;
    socket->streams = realloc(socket->streams, socket->streams_max_len * sizeof(udx_stream_t *));
  }

  handle->status = 0;

  *local_id = handle->local_id = id;
  handle->remote_id = 0;
  handle->set_id = socket->streams_len++;
  handle->socket = socket;

  socket->streams[handle->set_id] = handle;

  handle->seq = 0;
  handle->ack = 0;
  handle->remote_acked = 0;

  handle->srtt = 0;
  handle->rttvar = 0;
  handle->rto = 1000;
  handle->rto_timeout = get_milliseconds() + handle->rto;

  handle->pkts_waiting = 0;
  handle->pkts_inflight = 0;
  handle->dup_acks = 0;
  handle->retransmits_waiting = 0;

  handle->inflight = 0;
  handle->ssthresh = 65535;
  handle->cwnd = 2 * UDX_MTU;
  handle->rwnd = 0;

  handle->stats_sacks = 0;
  handle->stats_pkts_sent = 0;
  handle->stats_fast_rt = 0;
  handle->stats_last_seq = 0;

  handle->on_read = NULL;
  handle->on_recv = NULL;
  handle->on_drain = NULL;
  handle->on_close = close_cb;

  // Add the socket to the active set
  udx__cirbuf_set(&(socket->streams_by_id), (udx_cirbuf_val_t *) handle);

  // Init stream write/read buffers
  udx__cirbuf_init(&(handle->outgoing), 16);
  udx__cirbuf_init(&(handle->incoming), 16);

  return 0;
}

// TODO: finish this
void
udx_stream_recv_start (udx_stream_t *handle, udx_stream_recv_cb cb) {
  handle->on_recv = cb;
}

// TODO: finish this
void
udx_stream_recv_stop (udx_stream_t *handle) {
  handle->on_recv = NULL;
}

// TODO: finish this
void
udx_stream_read_start (udx_stream_t *handle, udx_stream_read_cb cb) {
  handle->on_read = cb;
}

// TODO: finish this
void
udx_stream_read_stop (udx_stream_t *handle) {
  handle->on_read = NULL;
}

int
udx_stream_check_timeouts (udx_stream_t *handle) {
  if (handle->remote_acked == handle->seq) return 0;

  const uint64_t now = handle->inflight ? get_milliseconds() : 0;

  if (now > handle->rto_timeout) {
    // Ensure it backs off until data is acked...
    handle->rto_timeout = now + 2 * handle->rto;

    // Consider all packet losts - seems to be the simple consensus across different stream impls
    // which we like cause it is nice and simple to implement.
    for (uint32_t seq = handle->remote_acked; seq != handle->seq; seq++) {
      udx_packet_t *pkt = (udx_packet_t *) udx__cirbuf_get(&(handle->outgoing), seq);

      if (pkt == NULL || pkt->status != UDX_PACKET_INFLIGHT) continue;

      if (pkt->transmits >= UDX_MAX_TRANSMITS) {
        handle->status |= UDX_STREAM_DESTROYED;
        close_maybe(handle, UDX_ERROR_TIMEOUT);
        return 1;
      }

      pkt->status = UDX_PACKET_WAITING;

      handle->inflight -= pkt->size;
      handle->pkts_waiting++;
      handle->pkts_inflight--;
      handle->retransmits_waiting++;
    }

    handle->cwnd = max(UDX_MTU, handle->cwnd / 2);

    printf("pkt loss! stream is congested, scaling back (requeued the full window)\n");
  }

  int err = flush_waiting_packets(handle);
  return err < 0 ? err : 0;
}

void
udx_stream_connect (udx_stream_t *handle, uint32_t remote_id, const struct sockaddr *remote_addr) {
  int already_connected = handle->status & UDX_STREAM_CONNECTED;

  handle->status |= UDX_STREAM_CONNECTED;

  handle->remote_id = remote_id;
  handle->remote_addr = *remote_addr;

  if (already_connected == 0) {
    // TODO: move this to read_start once we have that
    handle->socket->readers++;
    update_poll(handle->socket);
  }
}

int
udx_stream_send (udx_stream_send_t *req, udx_stream_t *handle, const uv_buf_t bufs[], unsigned int bufs_len, udx_stream_send_cb cb) {
  assert(bufs_len == 1);

  req->handle = handle;
  req->on_send = cb;

  udx_t *socket = handle->socket;
  udx_packet_t *pkt = &(req->pkt);

  init_stream_packet(pkt, UDX_HEADER_MESSAGE, handle, &bufs[0]);

  pkt->status = UDX_PACKET_SENDING;
  pkt->type = UDX_PACKET_STREAM_SEND;
  pkt->ctx = req;
  pkt->transmits = 0;

  pkt->fifo_gc = udx__fifo_push(&(socket->send_queue), pkt);
  return update_poll(socket);
}

void
udx_stream_write_resume (udx_stream_t *handle, udx_stream_drain_cb drain_cb) {
  handle->on_drain = drain_cb;
}

int
udx_stream_write (udx_stream_write_t *req, udx_stream_t *handle, const uv_buf_t bufs[], unsigned int bufs_len, udx_stream_ack_cb ack_cb) {
  assert(bufs_len == 1);

  req->packets = 0;
  req->handle = handle;
  req->on_ack = ack_cb;

  // if this is the first inflight packet, we should "restart" rto timer
  if (handle->inflight == 0) {
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
    pkt->ctx = req;

    handle->seq++;
    req->packets++;

    buf.len -= buf_partial_len;
    buf.base += buf_partial_len;

    udx__cirbuf_set(&(handle->outgoing), (udx_cirbuf_val_t *) pkt);

    // If we are not the first packet in the queue, wait to send us until the queue is flushed...
    if (handle->pkts_waiting++ > 0) continue;
    err = send_data_packet(handle, pkt);
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
    pkt->ctx = req;

    handle->seq++;
    req->packets++;

    buf.len -= buf_partial_len;
    buf.base += buf_partial_len;

    udx__cirbuf_set(&(handle->outgoing), (udx_cirbuf_val_t *) pkt);

    // If we are not the first packet in the queue, wait to send us until the queue is flushed...
    if (handle->pkts_waiting++ > 0) continue;
    err = send_data_packet(handle, pkt);
  } while (buf.len > 0 || err < 0);

  return err;
}

int
udx_stream_destroy (udx_stream_t *handle) {
  if ((handle->status & UDX_STREAM_CONNECTED) == 0) {
    handle->status |= UDX_STREAM_DESTROYED;
    close_maybe(handle, UDX_ERROR_DESTROYED);
    return 0;
  }

  handle->status |= UDX_STREAM_DESTROYING;

  clear_outgoing_packets(handle);

  udx_packet_t *pkt = malloc(sizeof(udx_packet_t));

  uv_buf_t buf = uv_buf_init(NULL, 0);

  init_stream_packet(pkt, UDX_HEADER_DESTROY, handle, &buf);

  pkt->status = UDX_PACKET_SENDING;
  pkt->type = UDX_PACKET_STREAM_DESTROY;
  pkt->ctx = handle;

  handle->seq++;

  udx__fifo_push(&(handle->socket->send_queue), pkt);

  int err = update_poll(handle->socket);
  return err < 0 ? err : 1;
}
