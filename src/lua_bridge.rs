use std::ffi::CStr;
use std::os::raw::c_char;

use tarantool::ffi::lua::{
    luaT_state, lua_State, lua_getglobal, lua_pushinteger, lua_setfield, lua_settop, lua_tostring,
    LUA_GLOBALSINDEX,
};

const LUA_FUNCS: &str = "local cartridge = require('cartridge');
local vshard = require('vshard')
local yaml = require('yaml')

function execute_sql(bucket_id, query)
    local data, err = vshard.router.call(
        bucket_id,
        'read',
        'box.execute',
        { query }
    )

    if err ~= nil then
        error(err)
    end

    return yaml.encode(data)
end

function get_cluster_schema()
  return cartridge.get_schema()
end";

extern "C" {
    pub fn luaL_loadbuffer(
        l: *mut lua_State,
        buff: *const c_char,
        sz: usize,
        name: *const c_char,
    ) -> i32;
    pub fn lua_pcall(state: *mut lua_State, nargs: i32, nresults: i32, msgh: i32) -> i32;
    pub fn lua_pushlstring(state: *mut lua_State, s: *const c_char, len: usize);

}

#[derive(Debug, Clone, Copy)]
pub struct LuaBridge {
    state: *mut lua_State,
}

#[allow(dead_code)]
impl LuaBridge {
    pub fn new() -> Self {
        let state = unsafe { luaT_state() };

        let global_name = unsafe { crate::c_ptr!("lua_bridge_initialized") };

        let mut res = unsafe {
            luaL_loadbuffer(
                state,
                LUA_FUNCS.as_ptr().cast::<i8>(),
                LUA_FUNCS.len(),
                crate::c_ptr!("helpers"),
            )
        };
        if res != 0 {
            panic!();
        };

        res = unsafe { lua_pcall(state, 0, 0, 0) };
        if res != 0 {
            panic!();
        };

        // set global lua state
        unsafe {
            lua_pushinteger(state, 1); // push the value onto the stack
            lua_setfield(state, LUA_GLOBALSINDEX, global_name); // save the global value
        };

        LuaBridge { state }
    }

    pub fn get_cluster_schema(self) -> String {
        unsafe {
            lua_getglobal(self.state, crate::c_ptr!("get_cluster_schema"));
        }

        let res = unsafe { lua_pcall(self.state, 0, 1, 0) };
        if res != 0 {
            panic!("{} {:?}", res, unsafe {
                CStr::from_ptr(lua_tostring(self.state, -1))
            });
        };

        // copy result string pointer from stack, because lua_tostring returns const char *
        let uri = unsafe { lua_tostring(self.state, -1) };
        let r = unsafe { CStr::from_ptr(uri) };

        // copy result string from raw pointer to safety variable
        let result = r.to_str().unwrap().to_string();

        //remove result pointer result from stack
        self.lua_pop(1);

        result
    }

    pub fn execute_sql(self, bucket_id: isize, query: &str) -> String {
        unsafe {
            lua_getglobal(self.state, crate::c_ptr!("execute_sql"));
            lua_pushinteger(self.state, bucket_id);

            // lua c api recommends `lua_pushlstring` for arbitrary strings and `lua_pushstring` for zero-terminated strings
            // as &str in Rust is byte array need use lua_pushlstring that doesn't transform input query string to zero-terminate CString
            lua_pushlstring(self.state, query.as_ptr().cast::<i8>(), query.len());
        }

        let res = unsafe { lua_pcall(self.state, 2, 1, 0) };
        if res != 0 {
            panic!("{} {:?}", res, unsafe {
                CStr::from_ptr(lua_tostring(self.state, -1))
            });
        };

        let uri = unsafe { lua_tostring(self.state, -1) };
        let r = unsafe { CStr::from_ptr(uri) };
        let result = r.to_str().unwrap().to_string();

        //remove result pointer result from stack
        self.lua_pop(1);

        result
    }

    fn lua_pop(self, n: i32) {
        unsafe { lua_settop(self.state, -n - 1) }
    }
}

#[macro_export]
macro_rules! c_ptr {
    ($s:literal) => {
        ::std::ffi::CStr::from_bytes_with_nul_unchecked(::std::concat!($s, "\0").as_bytes())
            .as_ptr()
    };
}
