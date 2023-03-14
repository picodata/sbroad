--- This module is used for configurating CPATH variable
--- for tests launched by test-run.py
local cpath = {}

local fio = require('fio')
local source_path = os.getenv("SOURCEDIR")

--- Append a C-library by specifying its dir and prefix (lib or etc).
--- if pefix is dropped it is consigered as an empty string
--- libdir is specifyed related to the project source dir.
cpath.append = function(libdir, prefix)
    prefix = prefix or ''
    package.cpath = fio.pathjoin(source_path, libdir) .. '/' .. prefix .. '?.so;' ..
                    fio.pathjoin(source_path, libdir) .. '/' .. prefix .. '?.dylib;' ..
                    package.cpath
end

return cpath
