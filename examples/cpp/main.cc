#include <iostream>

#include "const.pb.h"
#include "expr_args.pb.h"
#include "picachv_interfaces.h"
#include "plan_args.pb.h"

using namespace PicachvMessages;

int main(int argc, const char **argv) {
  SelectArgument arg;

  *arg.mutable_input_uuid() = "abcd";
  *arg.mutable_pred_uuid() = "efgh";

  PlanArgument parg;
  *parg.mutable_select() = arg;

  int ret = init_monitor();
  std::cout << "ret is " << ret << "\n";

  uint8_t buf[16] = {0};
  ret = open_new(buf, sizeof(buf));
  std::cout << "ret is " << ret << "\n";

  for (int i = 0; i < 16; i++) {
    std::cout << "buf[" << i << "] is " << (int)buf[i] << "\n";
  }

  uint8_t uuid[16] = {0};
  ret =
      build_plan(buf, sizeof(buf), (uint8_t *)parg.SerializeAsString().c_str(),
                 parg.ByteSizeLong(), uuid, sizeof(uuid));

  std::cout << "ret is " << ret << "\n";

  ret = execute(buf, sizeof(buf));

  std::cout << "ret is " << ret << "\n";

  return 0;
}
