#ifndef _PICACHV_INTERFACES_H
#define _PICACHV_INTERFACES_H

#include <cstdint>

typedef int32_t (*callback_t)(uint8_t *buf, std::size_t buf_len);

extern "C" {
int init_monitor();
int open_new(uint8_t *uuid, std::size_t uuid_len);

/**
 * This interface is used to tell the monitor to construct a plan on its side.
 *
 * @param [in] ctx_uuid The pointer to the UUID buffer of the context.
 * @param [in] ctx_uuid_len The length of the UUID buffer.
 * @param [in] arg The pointer to the argument (protobuf) buffer.
 * @param [in] arg_len The length of the protobuf struct.
 * @param [out] uuid The buffer used to receive the newly constructed plan's
 * UUID.
 * @param [in] uuid_len The length of that buffer.
 * @param [in] cb The callback that invokes thephysical executor.
 *                @see callback_t.
 */
int build_plan(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len, uint8_t *arg,
               std::size_t arg_len, uint8_t *uuid, std::size_t uuid_len);
/**
 * @brief This interface called whenever a physical executor is about to be performed.
 * 
 * @param ctx_uuid 
 * @param ctx_uuid_len 
 * @param plan_uuid 
 * @param plan_uuid_len 
 * @return int 
 */
int execute_prologue(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                     const uint8_t *plan_uuid, std::size_t plan_uuid_len);
}

#endif
