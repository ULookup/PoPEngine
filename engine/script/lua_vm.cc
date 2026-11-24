// 调用 Lua C API 创建和关闭虚拟机，将分配失败报告给分片启动方。

#include "engine/script/lua_vm.h"

#include <lua.hpp>
#include <stdexcept>

namespace pop {

void LuaCloser::operator()(lua_State* state) const {
  lua_close(state);
}

LuaVm CreateLuaVm() {
  LuaVm vm(luaL_newstate());
  if (!vm) {
    throw std::runtime_error("Lua VM allocation failed");
  }
  return vm;
}

}  // namespace pop
