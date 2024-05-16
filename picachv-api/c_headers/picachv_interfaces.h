#ifndef _PICACHV_INTERFACES_H
#define _PICACHV_INTERFACES_H

#include <cstdint>

/**
 * @brief The error code returned from the Rust code.
 *
 */
enum ErrorCode {
  /// @brief The operation is successful.
  Success = 0,
  /// @brief The operation is invalid.
  InvalidOperation = 1,
  /// @brief The serialization error.
  SerializeError = 2,
  /// @brief The requested object is not found.
  NoEntry = 3,
  /// @brief The privacy breach is detected.
  PrivacyBreach = 4,
  /// @brief The monitor is already opened.
  Already = 5
};

extern "C" {
/**
 * @brief Initialize the global instance of the monitor.
 *
 * @return ErrorCode
 */
ErrorCode init_monitor();

/**
 * @brief Opens a new context.
 *
 * @param [out] uuid The buffer for holding the UUID of the new context.
 * @param [in] uuid_len The length of the UUID buffer.
 * @return ErrorCode
 */
ErrorCode open_new(uint8_t *uuid, std::size_t uuid_len);

/**
 * @brief Register a new policy guarded dataframe into the context.
 *
 * @param [in] ctx_uuid The UUID of the context.
 * @param [in] ctx_uuid_len The length of the context UUID.
 * @param [in] dataframe The byte array of the dataframe.
 * @param [in] dataframe_len  The length of the dataframe.
 * @param [out] uuid The buffer for holding the UUID of the policy guarded
 * dataframe.
 * @param [in] uuid_len The length of the UUID buffer.
 * @return ErrorCode
 */
ErrorCode register_policy_dataframe(const uint8_t *ctx_uuid,
                                    std::size_t ctx_uuid_len,
                                    const uint8_t *dataframe,
                                    std::size_t dataframe_len, uint8_t *uuid,
                                    std::size_t uuid_len);

/**
 * @brief Constructs the expression out of the argument which is a serialized
 * protobuf byte array.
 *
 * @param [in] ctx_uuid The UUID of the context.
 * @param [in] ctx_uuid_len The length of the context UUID.
 * @param [in] args The byte array of the serialized protobuf.
 * @param [in] args_len The length of the serialized protobuf.
 * @param [out] expr_uuid The buffer for holding the UUID of the expression.
 * @param [in] expr_uuid_len The length of the expression UUID buffer.
 * @return ErrorCode
 */
ErrorCode expr_from_args(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                         const uint8_t *args, std::size_t args_len,
                         uint8_t *expr_uuid, std::size_t expr_uuid_len);

/**
 * @brief Reifies the values of an expression if polich checking needs doing so.
 *
 * @param [in] ctx_uuid The UUID of the context.
 * @param [in] ctx_uuid_len The length of the context UUID.
 * @param [in] expr_uuid The UUID of the expression.
 * @param [in] expr_uuid_len The length of the expression UUID.
 * @param [in] value The byte array of the values (in Apache Arrow format).
 * @param [in] value_len The length of the values.
 * @return ErrorCode
 *
 * @note See https://arrow.apache.org/docs/format/Columnar.html
 */
ErrorCode reify_expression(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                           const uint8_t *expr_uuid, std::size_t expr_uuid_len,
                           const uint8_t *value, std::size_t value_len);
}

#endif
