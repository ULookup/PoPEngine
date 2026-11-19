// 声明执行分片使用的 Lua VM 创建接口和独占句柄，句柄析构时关闭 VM。

#pragma once

#include <memory>
// Lua C API 提供的虚拟机状态类型。
struct lua_State;

namespace pop {
// LuaVm 的删除器，将句柄析构转换为 Lua C API 的关闭操作。
struct LuaCloser {
  // 关闭 state 并释放这个 VM 持有的 Lua 资源。
  void operator()(lua_State* state) const;
};
// 由单个执行分片独占的 VM 句柄。
using LuaVm = std::unique_ptr<lua_State, LuaCloser>;
// 分配一个新的 Lua 状态并返回独占句柄，分配失败时抛出异常。
LuaVm CreateLuaVm();
}  // namespace pop
