#ifndef TARANTOOL_SWIM_PROTO_H_INCLUDED
#define TARANTOOL_SWIM_PROTO_H_INCLUDED
/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "trivia/util.h"
#include "uuid/tt_uuid.h"
#include <arpa/inet.h>
#include <stdbool.h>

enum {
	/** Reserve 272 bytes for headers. */
	MAX_PAYLOAD_SIZE = 1200,
};

/**
 * SWIM binary protocol structures and helpers. Below is a picture
 * of a SWIM message template:
 *
 * +----------Meta section, handled by transport level-----------+
 * | {                                                           |
 * |     SWIM_META_TARANTOOL_VERSION: uint, Tarantool version ID,|
 * |     SWIM_META_SRC_ADDRESS: uint, ip,                        |
 * |     SWIM_META_SRC_PORT: uint, port,                         |
 * |     SWIM_META_ROUTING: {                                    |
 * |         SWIM_ROUTE_SRC_ADDRESS: uint, ip,                   |
 * |         SWIM_ROUTE_SRC_PORT: uint, port,                    |
 * |         SWIM_ROUTE_DST_ADDRESS: uint, ip,                   |
 * |         SWIM_ROUTE_DST_PORT: uint, port                     |
 * |     }                                                       |
 * | }                                                           |
 * +-------------------Protocol logic section--------------------+
 * | {                                                           |
 * |     SWIM_SRC_UUID: 16 byte UUID,                            |
 * |                                                             |
 * |                 AND                                         |
 * |                                                             |
 * |     SWIM_FAILURE_DETECTION: {                               |
 * |         SWIM_FD_MSG_TYPE: uint, enum swim_fd_msg_type,      |
 * |         SWIM_FD_INCARNATION: uint                           |
 * |     },                                                      |
 * |                                                             |
 * |               OR/AND                                        |
 * |                                                             |
 * |     SWIM_DISSEMINATION: [                                   |
 * |         {                                                   |
 * |             SWIM_MEMBER_STATUS: uint, enum member_status,   |
 * |             SWIM_MEMBER_ADDRESS: uint, ip,                  |
 * |             SWIM_MEMBER_PORT: uint, port,                   |
 * |             SWIM_MEMBER_UUID: 16 byte UUID,                 |
 * |             SWIM_MEMBER_INCARNATION: uint                   |
 * |         },                                                  |
 * |         ...                                                 |
 * |     ],                                                      |
 * |                                                             |
 * |               OR/AND                                        |
 * |                                                             |
 * |     SWIM_ANTI_ENTROPY: [                                    |
 * |         {                                                   |
 * |             SWIM_MEMBER_STATUS: uint, enum member_status,   |
 * |             SWIM_MEMBER_ADDRESS: uint, ip,                  |
 * |             SWIM_MEMBER_PORT: uint, port,                   |
 * |             SWIM_MEMBER_UUID: 16 byte UUID,                 |
 * |             SWIM_MEMBER_INCARNATION: uint                   |
 * |         },                                                  |
 * |         ...                                                 |
 * |     ],                                                      |
 * |                                                             |
 * |               OR/AND                                        |
 * |                                                             |
 * |     SWIM_QUIT: {                                            |
 * |         SWIM_QUIT_INCARNATION: uint                         |
 * |     }                                                       |
 * | }                                                           |
 * +-------------------------------------------------------------+
 */

enum swim_member_status {
	/** The instance is ok, responds to requests. */
	MEMBER_ALIVE = 0,
	/**
	 * If a member has not responded to a ping, it is declared
	 * as suspected to be dead. After more failed pings it
	 * is finaly dead.
	 */
	MEMBER_SUSPECTED,
	/**
	 * The member is considered to be dead. It will disappear
	 * from the membership, if it is not pinned.
	 */
	MEMBER_DEAD,
	/** The member has voluntary left the cluster. */
	MEMBER_LEFT,
	swim_member_status_MAX,
};

extern const char *swim_member_status_strs[];

/**
 * SWIM member attributes from anti-entropy and dissemination
 * messages.
 */
struct swim_member_def {
	struct tt_uuid uuid;
	struct tt_uuid old_uuid;
	struct sockaddr_in addr;
	uint64_t incarnation;
	enum swim_member_status status;
	const char *payload;
	int payload_size;
	/**
	 * Zero payload size does not mean that payload is not
	 * specified. It can be just empty.
	 */
	bool is_payload_specified;
};

/** Initialize the definition with default values. */
void
swim_member_def_create(struct swim_member_def *def);

/**
 * Decode member definition from a MessagePack buffer.
 * @param[out] def Definition to decode into.
 * @param[in][out] pos Start of the MessagePack buffer.
 * @param end End of the MessagePack buffer.
 * @param prefix A prefix of an error message to use for
 *        diag_set, when something is wrong.
 *
 * @retval 0 Success.
 * @retval -1 Error.
 */
int
swim_member_def_decode(struct swim_member_def *def, const char **pos,
		       const char *end, const char *prefix);

/**
 * Main round messages can carry merged failure detection,
 * anti-entropy, dissemination messages. With these keys the
 * components can be distinguished from each other.
 */
enum swim_body_key {
	SWIM_SRC_UUID = 0,
	SWIM_ANTI_ENTROPY,
	SWIM_FAILURE_DETECTION,
	SWIM_DISSEMINATION,
	SWIM_QUIT,
};

/**
 * One of SWIM packet body components - SWIM_SRC_UUID. It is not
 * in the meta section, handled by the transport, because the
 * transport has nothing to do with UUIDs - it operates by IP/port
 * only. This component shall be first in message's body.
 */
struct PACKED swim_src_uuid_bin {
	/** mp_encode_uint(SWIM_SRC_UUID) */
	uint8_t k_uuid;
	/** mp_encode_bin(UUID_LEN) */
	uint8_t m_uuid;
	uint8_t m_uuid_len;
	uint8_t v_uuid[UUID_LEN];
};

/** Initialize source UUID section. */
void
swim_src_uuid_bin_create(struct swim_src_uuid_bin *header,
			 const struct tt_uuid *uuid);

/** {{{                Failure detection component              */

/** Failure detection component keys. */
enum swim_fd_key {
	/** Type of the failure detection message: ping or ack. */
	SWIM_FD_MSG_TYPE,
	/**
	 * Incarnation of the sender. To make the member alive if
	 * it was considered to be dead, but ping/ack with greater
	 * incarnation was received from it.
	 */
	SWIM_FD_INCARNATION,
};

/** Failure detection message type. */
enum swim_fd_msg_type {
	SWIM_FD_MSG_PING,
	SWIM_FD_MSG_ACK,
	swim_fd_msg_type_MAX,
};

extern const char *swim_fd_msg_type_strs[];

/** SWIM failure detection MessagePack header template. */
struct PACKED swim_fd_header_bin {
	/** mp_encode_uint(SWIM_FAILURE_DETECTION) */
	uint8_t k_header;
	/** mp_encode_map(2) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_FD_MSG_TYPE) */
	uint8_t k_type;
	/** mp_encode_uint(enum swim_fd_msg_type) */
	uint8_t v_type;

	/** mp_encode_uint(SWIM_FD_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

/** Initialize failure detection section. */
void
swim_fd_header_bin_create(struct swim_fd_header_bin *header,
			  enum swim_fd_msg_type type, uint64_t incarnation);

/** A decoded failure detection message. */
struct swim_failure_detection_def {
	/** Type of the message. */
	enum swim_fd_msg_type type;
	/** Incarnation of the sender. */
	uint64_t incarnation;
};

/**
 * Decode failure detection from a MessagePack buffer.
 * @param[out] def Definition to decode into.
 * @param[in][out] pos Start of the MessagePack buffer.
 * @param end End of the MessagePack buffer.
 * @param prefix A prefix of an error message to use for diag_set,
 *        when something is wrong.
 *
 * @retval 0 Success.
 * @retval -1 Error.
 */
int
swim_failure_detection_def_decode(struct swim_failure_detection_def *def,
				  const char **pos, const char *end,
				  const char *prefix);

/** }}}               Failure detection component               */

/** {{{                  Anti-entropy component                 */

/**
 * Attributes of each record of a broadcasted member table. Just
 * the same as some of struct swim_member attributes.
 */
enum swim_member_key {
	SWIM_MEMBER_STATUS = 0,
	SWIM_MEMBER_ADDRESS,
	SWIM_MEMBER_PORT,
	SWIM_MEMBER_UUID,
	SWIM_MEMBER_INCARNATION,
	SWIM_MEMBER_OLD_UUID,
	SWIM_MEMBER_PAYLOAD,
	swim_member_key_MAX,
};

/** SWIM anti-entropy MessagePack header template. */
struct PACKED swim_anti_entropy_header_bin {
	/** mp_encode_uint(SWIM_ANTI_ENTROPY) */
	uint8_t k_anti_entropy;
	/** mp_encode_array(...) */
	uint8_t m_anti_entropy;
	uint16_t v_anti_entropy;
};

/** Initialize SWIM_ANTI_ENTROPY header. */
void
swim_anti_entropy_header_bin_create(struct swim_anti_entropy_header_bin *header,
				    uint16_t batch_size);

/**
 * SWIM member MessagePack template. Represents one record in
 * anti-entropy section.
 */
struct PACKED swim_member_bin {
	/** mp_encode_map(5) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_MEMBER_STATUS) */
	uint8_t k_status;
	/** mp_encode_uint(enum member_status) */
	uint8_t v_status;

	/** mp_encode_uint(SWIM_MEMBER_ADDRESS) */
	uint8_t k_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_addr;
	uint32_t v_addr;

	/** mp_encode_uint(SWIM_MEMBER_PORT) */
	uint8_t k_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_port;
	uint16_t v_port;

	/** mp_encode_uint(SWIM_MEMBER_UUID) */
	uint8_t k_uuid;
	/** mp_encode_bin(UUID_LEN) */
	uint8_t m_uuid;
	uint8_t m_uuid_len;
	uint8_t v_uuid[UUID_LEN];

	/** mp_encode_uint(SWIM_MEMBER_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;

	/** mp_encode_uint(SWIM_MEMBER_PAYLOAD) */
	uint8_t k_payload;
	/** mp_encode_bin(16bit bin header) */
	uint8_t m_payload_size;
	uint16_t v_payload_size;
	/** Payload data ... */
};

/** Initialize antri-entropy record. */
void
swim_member_bin_create(struct swim_member_bin *header);

/**
 * Since usually there are many members, it is faster to reset a
 * few fields in an existing template, then each time create a
 * new template. So the usage pattern is create(), fill(),
 * fill() ... .
 */
void
swim_member_bin_fill(struct swim_member_bin *header,
		     const struct sockaddr_in *addr, const struct tt_uuid *uuid,
		     enum swim_member_status status, uint64_t incarnation,
		     uint16_t payload_size);

/** }}}                  Anti-entropy component                 */

/** {{{                 Dissemination component                 */

/** SWIM dissemination MessagePack template. */
struct PACKED swim_diss_header_bin {
	/** mp_encode_uint(SWIM_DISSEMINATION) */
	uint8_t k_header;
	/** mp_encode_array() */
	uint8_t m_header;
	uint16_t v_header;
};

/** Initialize dissemination header. */
void
swim_diss_header_bin_create(struct swim_diss_header_bin *header,
			    uint16_t batch_size);

/** SWIM event MessagePack template. */
struct PACKED swim_event_bin {
	/** mp_encode_map(5, or 6, or 7) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_MEMBER_STATUS) */
	uint8_t k_status;
	/** mp_encode_uint(enum member_status) */
	uint8_t v_status;

	/** mp_encode_uint(SWIM_MEMBER_ADDRESS) */
	uint8_t k_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_addr;
	uint32_t v_addr;

	/** mp_encode_uint(SWIM_MEMBER_PORT) */
	uint8_t k_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_port;
	uint16_t v_port;

	/** mp_encode_uint(SWIM_MEMBER_UUID) */
	uint8_t k_uuid;
	/** mp_encode_bin(UUID_LEN) */
	uint8_t m_uuid;
	uint8_t m_uuid_len;
	uint8_t v_uuid[UUID_LEN];

	/** mp_encode_uint(SWIM_MEMBER_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

/** Initialize dissemination record. */
void
swim_event_bin_create(struct swim_event_bin *header);

/**
 * Since usually there are many evnets, it is faster to reset a
 * few fields in an existing template, then each time create a
 * new template. So the usage pattern is create(), fill(),
 * fill() ... .
 */
void
swim_event_bin_fill(struct swim_event_bin *header,
		    enum swim_member_status status,
		    const struct sockaddr_in *addr, const struct tt_uuid *uuid,
		    uint64_t incarnation, int old_uuid_ttl, int payload_ttl);

/** Optional attribute of an event - old UUID of a member. */
struct swim_old_uuid_bin {
	/** mp_encode_uint(SWIM_MEMBER_OLD_UUID) */
	uint8_t k_uuid;
	/** mp_encode_bin(UUID_LEN) */
	uint8_t m_uuid;
	uint8_t m_uuid_len;
	uint8_t v_uuid[UUID_LEN];
};

/** Initialize old UUID field. */
void
swim_old_uuid_bin_create(struct swim_old_uuid_bin *header);

/**
 * Set mutable fields of the field, by the same principle as event
 * filling.
 */
void
swim_old_uuid_bin_fill(struct swim_old_uuid_bin *header,
		       const struct tt_uuid *uuid);

/** }}}                 Dissemination component                 */

/** {{{                     Meta component                      */

/**
 * Meta component keys, completely handled by the transport level.
 */
enum swim_meta_key {
	/**
	 * Version is now unused, but in future can help in
	 * the protocol improvement, extension.
	 */
	SWIM_META_TARANTOOL_VERSION = 0,
	/**
	 * Source IP/port are stored in body of UDP packet despite
	 * the fact that UDP has them in its header. This is
	 * because
	 *     - packet body is going to be encrypted, but header
	 *       is still open and anybody can catch the packet,
	 *       change source IP/port, and therefore execute
	 *       man-in-the-middle attack;
	 *
	 *     - some network filters can change the address to an
	 *       address of a router or another device.
	 */
	SWIM_META_SRC_ADDRESS,
	SWIM_META_SRC_PORT,
	SWIM_META_ROUTING,
};

/**
 * Each SWIM packet carries meta info, which helps to determine
 * SWIM protocol version, final packet destination and any other
 * internal details, not linked with etalon SWIM protocol.
 *
 * The meta header is mandatory, preceeds main protocol data as a
 * separate MessagePack map.
 */
struct PACKED swim_meta_header_bin {
	/** mp_encode_map(3 or 4) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_META_TARANTOOL_VERSION) */
	uint8_t k_version;
	/** mp_encode_uint(tarantool_version_id()) */
	uint8_t m_version;
	uint32_t v_version;

	/** mp_encode_uint(SWIM_META_SRC_ADDRESS) */
	uint8_t k_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_addr;
	uint32_t v_addr;

	/** mp_encode_uint(SWIM_META_SRC_PORT) */
	uint8_t k_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_port;
	uint16_t v_port;
};

/** Initialize meta section. */
void
swim_meta_header_bin_create(struct swim_meta_header_bin *header,
			    const struct sockaddr_in *src, bool has_routing);

/** Meta definition. */
struct swim_meta_def {
	/** Tarantool version. */
	uint32_t version;
	/** Source of the message. */
	struct sockaddr_in src;
	/** Route source and destination. */
	bool is_route_specified;
	struct {
		struct sockaddr_in src;
		struct sockaddr_in dst;
	} route;
};

/**
 * Decode meta section into its definition object.
 * @param[out] def Definition to decode into.
 * @param[in][out] pos MessagePack buffer to decode.
 * @param end End of the MessagePack buffer.
 *
 * @retval 0 Success.
 * @retval -1 Error.
 */
int
swim_meta_def_decode(struct swim_meta_def *def, const char **pos,
		     const char *end);

enum swim_route_key {
	/**
	 * True source of the packet. Can be different from the
	 * packet sender. It is expected that the answer should
	 * be sent back to this address. Maybe indirectly through
	 * the same proxy.
	 */
	SWIM_ROUTE_SRC_ADDRESS = 0,
	SWIM_ROUTE_SRC_PORT,
	/**
	 * True destination of the packet. Can be different from
	 * this instance, receiver. If it is for another instance,
	 * then this packet is forwarded to the latter.
	 */
	SWIM_ROUTE_DST_ADDRESS,
	SWIM_ROUTE_DST_PORT,
	swim_route_key_MAX,
};

/** Route section template. Describes source, destination. */
struct PACKED swim_route_bin {
	/** mp_encode_uint(SWIM_ROUTING) */
	uint8_t k_routing;
	/** mp_encode_map(4) */
	uint8_t m_routing;

	/** mp_encode_uint(SWIM_ROUTE_SRC_ADDRESS) */
	uint8_t k_src_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_src_addr;
	uint32_t v_src_addr;

	/** mp_encode_uint(SWIM_ROUTE_SRC_PORT) */
	uint8_t k_src_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_src_port;
	uint16_t v_src_port;

	/** mp_encode_uint(SWIM_ROUTE_DST_ADDRESS) */
	uint8_t k_dst_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_dst_addr;
	uint32_t v_dst_addr;

	/** mp_encode_uint(SWIM_ROUTE_DST_PORT) */
	uint8_t k_dst_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_dst_port;
	uint16_t v_dst_port;
};

/** Initialize routing section. */
void
swim_route_bin_create(struct swim_route_bin *route,
		      const struct sockaddr_in *src,
		      const struct sockaddr_in *dst);

/** }}}                     Meta component                      */

enum swim_quit_key {
	/** Incarnation to ignore old quit messages. */
	SWIM_QUIT_INCARNATION = 0,
};

/** Quit section. Describes voluntary quit from the cluster. */
struct PACKED swim_quit_bin {
	/** mp_encode_uint(SWIM_QUIT) */
	uint8_t k_quit;
	/** mp_encode_map(1) */
	uint8_t m_quit;

	/** mp_encode_uint(SWIM_QUIT_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

/** Initialize quit section. */
void
swim_quit_bin_create(struct swim_quit_bin *header, uint64_t incarnation);

/**
 * Helpers to decode some values - map, array, etc with
 * appropriate checks. All of them set diagnostics on an error
 * with a specified message prefix and a parameter name.
 */

int
swim_decode_map(const char **pos, const char *end, uint32_t *size,
		const char *prefix, const char *param_name);

int
swim_decode_array(const char **pos, const char *end, uint32_t *size,
		  const char *prefix, const char *param_name);

int
swim_decode_uint(const char **pos, const char *end, uint64_t *value,
		 const char *prefix, const char *param_name);

int
swim_decode_uuid(struct tt_uuid *uuid, const char **pos, const char *end,
		 const char *prefix, const char *param_name);

#endif /* TARANTOOL_SWIM_PROTO_H_INCLUDED */
