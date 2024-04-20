#include <iostream>

#include "expr_args.pb.h"
#include "plan_args.pb.h"
#include "const.pb.h"
#include "picachv_interfaces.h"

using namespace PicachvMessages;

int main(int argc, const char **argv) { 
  SelectArgument arg;

  arg.set_input_uuid("abcd");
  arg.set_pred_uuid("fegh");

  int ret = init_monitor();

  std::cout << "ret is " << ret << "\n";

  return 0;
}
