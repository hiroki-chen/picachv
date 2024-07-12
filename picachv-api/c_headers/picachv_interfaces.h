#ifndef _PICACHV_INTERFACES_H
#define _PICACHV_INTERFACES_H

#include <cstdint>

#define PICACHV_UUID_LEN 16

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
  /// @brief The monitor is already opened or something already exists.
  Already = 5,
  /// @brief The file is not found.
  FileNotFound = 6,
};

extern "C" {
/**
 * @brief Get the last error message. Please be aware that the error message
 * does NOT include the trailing zero '\0'.
 *
 * @param [out] err_msg The buffer for holding the error message.
 * @param [in, out] err_msg_len The length of the error message buffer.
 */
void last_error(uint8_t *err_msg, std::size_t *err_msg_len);

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

ErrorCode register_policy_dataframe_from_row_group(
    const uint8_t *ctx_uuid, std::size_t ctx_uuid_len, const uint8_t *path,
    std::size_t path_len, std::size_t row_group, uint8_t *df_uuid,
    std::size_t df_uuid_len, const std::size_t *projection,
    std::size_t projection_len, const bool *selection,
    std::size_t selection_len);

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

/**
 * @brief Creates a sliced dataframe.
 *
 * @param [in] ctx_uuid The UUID of the context.
 * @param [in] ctx_uuid_len The length of the context UUID.
 * @param [in] df_uuid The UUID of the dataframe.
 * @param [in] df_uuid_len The length of the dataframe UUID.
 * @param start The start index of the slice.
 * @param end The end index of the slice.
 * @param [out] slice_uuid The buffer for holding the UUID of the sliced
 * @param [in] slice_uuid_len The length of the slice UUID buffer.
 * @return ErrorCode
 */
ErrorCode create_slice(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                       const uint8_t *df_uuid, std::size_t df_uuid_len,
                       uint64_t start, uint64_t end, uint8_t *slice_uuid,
                       std::size_t slice_uuid_len);

/**
 * @brief Finalize should be called whenever the analytical result is collected.
 * This function makes sure that the policy should be met.
 *
 * @param [in] ctx_uuid The UUID of the context.
 * @param [in] ctx_uuid_len The length of the context UUID.
 * @param [in] df_uuid The UUID of the dataframe.
 * @param [in] df_uuid_len The length of the dataframe UUID.
 * @return ErrorCode
 */
ErrorCode finalize(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                   const uint8_t *df_uuid, std::size_t df_uuid_len);

/**
 * @brief Do an early projection on the dataframe.
 *
 * @param ctx_uuid
 * @param ctx_uuid_len
 * @param df_uuid
 * @param df_uuid_len
 * @param project_list
 * @param project_list_len
 * @param result_uuid
 * @param result_uuid_len
 * @return ErrorCode
 */
ErrorCode early_projection(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                           const uint8_t *df_uuid, std::size_t df_uuid_len,
                           const std::size_t *project_list,
                           std::size_t project_list_len, uint8_t *result_uuid,
                           std::size_t result_uuid_len);

/**
 * @brief Check if the policy is met after the execution.
 *
 * @param ctx_uuid
 * @param ctx_uuid_len
 * @param plan_arg
 * @param plan_arg_len
 * @param df_uuid
 * @param df_uuid_len
 * @param output
 * @param output_len
 * @return ErrorCode
 */
ErrorCode execute_epilogue(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                           const uint8_t *plan_arg, std::size_t plan_arg_len,
                           const uint8_t *df_uuid, std::size_t df_uuid_len,
                           uint8_t *output, std::size_t output_len);

/**
 * @brief Print the policy-guarded dataframe.
 *
 * @param ctx_uuid
 * @param ctx_uuid_len
 * @param df_uuid
 * @param df_uuid_len
 * @return ErrorCode
 */
ErrorCode debug_print_df(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                         const uint8_t *df_uuid, std::size_t df_uuid_len);

/**
 * @brief Enable the profiling.
 *
 * @param ctx_uuid
 * @param ctx_uuid_len
 * @param enable
 * @return ErrorCode
 */
ErrorCode enable_profiling(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                           bool enable);

/**
 * @brief Enable the tracing.
 *
 * @param ctx_uuid
 * @param ctx_uuid_len
 * @param enable
 * @return ErrorCode
 */
ErrorCode enable_tracing(const uint8_t *ctx_uuid, std::size_t ctx_uuid_len,
                         bool enable);
}
#endif
