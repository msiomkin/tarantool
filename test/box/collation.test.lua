env = require('test_run')
test_run = env.new()

--
-- gh-4007 Feature request for a new collation
--
-- Ensure all default collations exist
box.space._collation.index.name:get{'unicode'};
box.space._collation.index.name:get{'unicode_ci'};
box.space._collation.index.name:get{'unicode_s2'};

-- Default unicode collation deals with russian letters
s = box.schema.space.create('t1');
s:format({{name='s1', type='string', collation = 'unicode'}});
s:create_index('pk', {unique = true, type='tree', parts={{'s1', collation = 'unicode'}}});
s:insert{'Ё'};
s:insert{'Е'};
s:insert{'ё'};
s:insert{'е'};
-- all 4 letters are in the table
s:select{};
s:drop();

-- unicode_ci collation doesn't distinguish russian letters 'Е' and 'Ё'
s = box.schema.space.create('t1');
s:format({{name='s1', type='string', collation = 'unicode_ci'}});
s:create_index('pk', {unique = true, type='tree', parts={{'s1', collation = 'unicode_ci'}}});
s:insert{'Ё'};
-- the following calls should fail
s:insert{'е'};
s:insert{'Е'};
s:insert{'ё'};
-- return single 'Ё'
s:select{};
s:drop();

-- unicode_s2 collation does distinguish russian letters 'Е' and 'Ё'
s = box.schema.space.create('t1');
s:format({{name='s1', type='string', collation = 'unicode_s2'}});
s:create_index('pk', {unique = true, type='tree', parts={{'s1', collation = 'unicode_s2'}}});
s:insert{'Ё'};
s:insert{'е'};
-- the following calls should fail
s:insert{'Е'};
s:insert{'ё'};
-- return two: 'Ё' and 'е'
s:select{};
s:drop();

