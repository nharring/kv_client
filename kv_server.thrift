namespace rs KVServer
namespace go KVServer

/**
* We'll use a couple general exception classes
* and then we can subspecialize them if necessary
*/
exception KeyNotFound {
  1: string key
}

exception ServiceException {
    1: string what,
    2: bool retryable,   // Should clients consider this a hard failure
}

struct KVObject {
  1: string key,
  2: string value,
}

service KVServer {
  bool set_key(1: KVObject kv) throws (1: ServiceException service_exception);
  string get_val(1: string key) throws  (1: KeyNotFound key_exception, 2: ServiceException service_exception);
  KVObject get_obj(1: string key) throws (1:KeyNotFound key_exception, 2: ServiceException service_exception);
  void del_key(1: string key);
}
