test_run = require('test_run')
inspector = test_run.new()
engine = inspector:get_cfg('engine')
iterations = 100000

function string_function()
    local random_number
    local random_string
    random_string = ""
    for x = 1,20,1 do
        random_number = math.random(65, 90)
        random_string = random_string .. string.char(random_number)
    end
    return random_string
end

function delete_insert(engine_name, iterations)
    local string_value
    if (box.space._space.index.name:select{'tester'}[1] ~= nil) then
        box.space.tester:drop()
    end
    box.schema.space.create('tester', {engine=engine_name})
    box.space.tester:create_index('primary',{type = 'tree', parts = {1, 'STR'}})
    local string_value_2
    local counter = 1
    while counter < iterations do
        local string_value = string_function()
        local string_table = box.space.tester.index.primary:select({string_value}, {iterator = 'EQ'})

        if string_table[1] == nil then
            -- print (1, ' insert', counter, string_value)
            box.space.tester:insert{string_value, counter}
            string_value_2 = string_value
        else
            string_value_2 = string_table[1][1]
        end

        if string_value_2 == nil then
            -- print (2, ' insert', counter, string_value)
            box.space.tester:insert{string_value, counter}
            string_value_2 = string_value
        end

        -- print (3, ' delete', counter, string_value_2)
        box.space.tester:delete{string_value_2}

        -- print (4, ' insert', counter, string_value_2)
        box.space.tester:insert{string_value_2, counter}

        counter = counter + 1
    end
    box.space.tester:drop()
    return {counter, string_value_2}
end

math.randomseed(1)
delete_insert(engine, iterations)
