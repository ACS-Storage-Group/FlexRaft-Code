#include "type.h"

#include <string>
namespace kv {
const std::string ToString(RequestType type) {
  switch (type) {
    case kPut:
      return "Put";
    case kGet:
      return "Get";
    case kDelete:
      return "Delete";
    case kDetectLeader:
      return "Detect Leader";
  }
  return "UnderterminedRequestType";
}

const std::string ToString(ErrorType type) {
  switch (type) {
    case kNotALeader:
      return "kNotLeader";
    case kKeyNotExist:
      return "KeyNotExist";
    case kEntryDeleted:
      return "EntryDeleted";
    case kRequestExecTimeout:
      return "RequestExecTimeout";
    case kOk:
      return "kOk";
    case kKVRequestTimesout:
      return "KVRequestTimesout";
    case kRPCCallFailed:
      return "RPCCallFailed";
    case kKVDecodeFail:
      return "KVDecodeFail";
  }
  return "UndeterminedErrorType";
}

const std::string ToString(const Request& req) {
  return ToString(req.type) + " " + "(key = " + req.key + " value = " + req.value + ")";
}
}  // namespace kv
