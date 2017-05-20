local tarantool = require("tarantool")
local os = require("os")
local _addmethod
local _exec
_exec = function(self, proc, ...)
  if type(proc) == "table" and proc.obj == self.obj then
    return self.obj.call(self.obj, "method", {
      self._method,
      proc._method,
      ...
    })
  else
    return self.obj.call(self.obj, "meta", {
      self._method,
      proc,
      ...
    })
  end
end
_addmethod = function(self, _method)
  local obj
  if not self._method then
    obj = self
    obj._method = _method
  else
    obj = {
      obj = self.obj
    }
    if #self._method == 0 then
      obj._method = _method
    else
      obj._method = self._method .. "." .. _method
    end
  end
  setmetatable(obj, {
    __index = _addmethod,
    __call = _exec
  })
  return obj
end
local Migrator
do
  local _class_0
  local _base_0 = {
    migration_up = function(self, migrations, index)
      if migrations[index].up(self) then
        local res, err = self.tar:insert("migrations", {
          index,
          os.time()
        })
        return not err, err
      else
        return nil, "migration_up " .. tostring(index) .. " failed"
      end
    end,
    migration_down = function(self, migrations, index)
      if migrations[index].down(self) then
        self.tar:delete("migrations", index)
        return true
      else
        return nil, "migration_down " .. tostring(index) .. " failed"
      end
    end,
    has_migration = function(self, index)
      local res, err = self.tar:select("migrations", "primary", index)
      return not err
    end,
    bootstrap = function(self)
      local res, err = self.tar:select("_space", "name", "migrations")
      if err then
        return migration_up(self.bootstrap_migration, "1")
      end
    end,
    load_from_file = function(self, fname)
      local func, err = loadfile(fname)
      if err then
        return nil, err
      end
      return nil
    end,
    create_raw = function(self, dir, name, template, ext)
      local fname = fio.pathjoin(dir, tostring(os.date('%Y%m%d%H%M%S')) .. "_" .. tostring(name or 'migration') .. "." .. tostring(ext))
      local fh, err = io.open(fname, "w")
      if not fh or err then
        return nil, err
      end
      fh:write(template)
      return fh:close()
    end,
    migrate_dir = function(self, dir)
      local info = fio.stat(dir)
      if not info or not (info:is_dir()) then
        return nil, "No such directory " .. tostring(dir)
      end
      io.stderr:write("processing migrations in " .. tostring(dir) .. "\n")
      local fnames = fio.glob(tostring(dir) .. "/*.lua")
      table.sort(fnames)
      for _, fname in ipairs(fnames) do
        io.stderr:write("opening file " .. tostring(fname) .. "\n")
        local index = string.match(fio.basename(fname), "(%d+)_.*%.lua")
        io.stderr:write("processing migration [" .. tostring(index) .. "]")
        do_migrations({
          [index] = dofile(fname)
        })
      end
    end,
    do_migrations = function(self, migrations)
      bootstrap(migrations)
      for index, updown in ipairs(migrations) do
        if not has_migration(index) then
          migrate_up(migrations, index)
        else
          log.info("skipping migration [" .. tostring(index) .. "]")
          local _ = true
        end
      end
    end
  }
  _base_0.__index = _base_0
  _class_0 = setmetatable({
    __init = function(self, args)
      local err
      self.tar, err = tarantool:new(args)
      self.meta = _addmethod({
        obj = self.tar
      }, "")
    end,
    __base = _base_0,
    __name = "Migrator"
  }, {
    __index = _base_0,
    __call = function(cls, ...)
      local _self_0 = setmetatable({}, _base_0)
      cls.__init(_self_0, ...)
      return _self_0
    end
  })
  _base_0.__class = _class_0
  local self = _class_0
  self.bootstrap_migration = {
    up = function(migrator)
      migrator.meta.box.schema.space.create('migrations')
      migrator.meta.box.space.migrations:create_index('primary', {
        if_not_exists = true,
        unique = true,
        type = 'TREE',
        parts = {
          1,
          'string'
        }
      })
      return migrator.meta.box.space.migrations:create_index('time', {
        if_not_exists = true,
        unique = true,
        type = 'TREE',
        parts = {
          2,
          'unsigned'
        }
      })
    end,
    down = function(migrator)
      return migrator.meta.box.space.migrations:drop()
    end
  }
  self.lua_template = "-- migration created by migrate.moon\n  return {\n    up = function(migrator) -- forward migration\n    end,\n    down = function(migrator) -- backward migration\n    end\n  }\n  "
  self.moon_template = "-- migration created by migrate.moon\n  {\n    up: (migrator) ->\n      -- forward migration\n    down: (migrator) ->\n      -- backward migration\n  }\n  "
  Migrator = _class_0
end
return Migrator
