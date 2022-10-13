_G.fiber_id = function ()
    local fiber = require('fiber')
    return fiber.id()
end
