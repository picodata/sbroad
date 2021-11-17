use std::ffi::CStr;
use std::os::raw::c_char;

use tarantool::ffi::lua::{
    lua_State, lua_getglobal, lua_pushinteger, lua_pushstring, lua_setfield, lua_settop,
    lua_tointeger, lua_tostring, LUA_GLOBALSINDEX,
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

#[allow(dead_code)]
pub fn init_cluster_functions(l: *mut lua_State) {
    let global_name = unsafe { crate::c_ptr!("cluster_functions_initialized") };

    // checking that lua code already was loaded
    if get_global(l, global_name) != 0 {
        return;
    }

    let mut res = unsafe {
        luaL_loadbuffer(
            l,
            LUA_FUNCS.as_ptr().cast::<i8>(),
            LUA_FUNCS.len(),
            crate::c_ptr!("helpers"),
        )
    };
    if res != 0 {
        panic!();
    };

    res = unsafe { lua_pcall(l, 0, 0, 0) };
    if res != 0 {
        panic!();
    };

    set_global(l, global_name, 1);
}

#[allow(dead_code)]
pub fn execute_sql(l: *mut lua_State, bucket_id: isize, query: &str) -> String {
    unsafe {
        lua_getglobal(l, crate::c_ptr!("execute_sql"));
        lua_pushinteger(l, bucket_id);
        lua_pushstring(l, query.as_ptr().cast::<i8>());
    }

    let res = unsafe { lua_pcall(l, 2, 1, 0) };
    if res != 0 {
        panic!("{} {:?}", res, unsafe {
            CStr::from_ptr(lua_tostring(l, -1))
        });
    };

    let uri = unsafe { lua_tostring(l, -1) };
    let r = unsafe { CStr::from_ptr(uri) };
    let result = r.to_str().unwrap().to_string();

    unsafe { lua_pop(l, 1) };

    result
}

pub fn get_cluster_schema(l: *mut lua_State) -> String {
    unsafe {
        lua_getglobal(l, crate::c_ptr!("get_cluster_schema"));
    }

    let res = unsafe { lua_pcall(l, 0, 1, 0) };
    if res != 0 {
        panic!("{} {:?}", res, unsafe {
            CStr::from_ptr(lua_tostring(l, -1))
        });
    };

    // copy result string pointer from stack, because lua_tostring returns const char *
    let uri = unsafe { lua_tostring(l, -1) };
    let r = unsafe { CStr::from_ptr(uri) };

    // copy result string from raw pointer to safety variable
    let result = r.to_str().unwrap().to_string();

    //remove result pointer result from stack
    unsafe { lua_pop(l, 1) };

    result
}

extern "C" {
    pub fn luaL_loadbuffer(
        l: *mut lua_State,
        buff: *const c_char,
        sz: usize,
        name: *const c_char,
    ) -> i32;
    pub fn lua_pcall(state: *mut lua_State, nargs: i32, nresults: i32, msgh: i32) -> i32;

}

pub unsafe fn lua_pop(state: *mut lua_State, n: i32) {
    lua_settop(state, -n - 1);
}

pub fn get_global(l: *mut lua_State, g: *const c_char) -> isize {
    unsafe { lua_getglobal(l, g) };
    //copy result value to safety variable, because lua_tointeger returns lua_Integer
    let v = unsafe { lua_tointeger(l, -1) }; // read the value on the stack
    unsafe { lua_pop(l, 1) }; // remove the value from the stack
    v
}

pub fn set_global(l: *mut lua_State, g: *const c_char, v: isize) {
    unsafe {
        lua_pushinteger(l, v); // push the value onto the stack
        lua_setfield(l, LUA_GLOBALSINDEX, g); // save the global value
    };
}

#[macro_export]
macro_rules! c_ptr {
    ($s:literal) => {
        ::std::ffi::CStr::from_bytes_with_nul_unchecked(::std::concat!($s, "\0").as_bytes())
            .as_ptr()
    };
}
