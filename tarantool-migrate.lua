local tarantool = require("tarantool")
local os = require("os")
local lfs = require("lfs")
local path = require("path")
local inspect = require("inspect")
local logging = require("logging")
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
  local _appender
  local _base_0 = {
    migration_up = function(self, migration, index, name)
      tar:disable_lookups()
      local res, err = migration.up(self)
      tar:enable_lookups()
      if res then
        local t = os.time()
        res, err = self.tar:insert("migrations", {
          index,
          t,
          name
        })
        return not err, err
      else
        return nil, "migration_up " .. tostring(index) .. " failed: " .. tostring(err)
      end
    end,
    migration_down = function(self, migration, index)
      tar:disable_lookups()
      local res, err = migration.down(self)
      tar:enable_lookups()
      if res then
        res, err = self.tar:delete("migrations", index)
        return not err, err
      else
        return nil, "migration_down " .. tostring(index) .. " failed: " .. tostring(err)
      end
    end,
    has_migration = function(self, index)
      local res, err = self.tar:select("migrations", "primary", index)
      return res[2], err
    end,
    check_bootstrapped = function(self)
      local res, err = self.tar:select("_space", "name", "migrations")
      return not (not res or #res == 0) and not err, err
    end,
    bootstrap = function(self)
      if self.bootstrapped then
        return true, nil
      end
      local err
      self.bootstrapped, err = self:check_bootstrapped()
      if not self.bootstrapped then
        self:migration_up(self.__class.bootstrap_migration, "1")
        self.bootstrapped, err = self:check_bootstrapped()
      end
      return self.bootstrapped, err
    end,
    load_from_file = function(self, fname)
      local func, err = loadfile(fname)
      if err then
        return nil, err
      end
      return nil
    end,
    open_or_create = function(self, dir)
      local info, err = lfs.attributes(dir)
      if not info then
        lfs.mkdir(dir)
        info, err = lfs.attributes(dir)
      end
      return info.mode == "directory", err
    end,
    create_raw = function(self, dir, name, template, ext)
      local exist, err = self:open_or_create(dir)
      if not exist then
        return nil, err
      end
      local fname = path.join(dir, tostring(os.date('%Y%m%d%H%M%S')) .. "_" .. tostring(name or 'migration') .. "." .. tostring(ext))
      local fh
      fh, err = io.open(fname, "w")
      if not fh or err then
        return nil, err
      end
      fh:write(template)
      return fh:close()
    end,
    create_moon = function(self, dir, name)
      return self:create_raw(dir or "migrate", name, self.__class.moon_template, "moon")
    end,
    create_lua = function(self, dir, name)
      return self:create_raw(dir or "migrate", name, self.__class.lua_template, "lua")
    end,
    _list_files_sorted = function(self, dir)
      local ndir = path.normalize(dir)
      local fullpath, err = path.isdir(ndir)
      if err or not fullpath then
        return nil, "No such directory " .. tostring(ndir)
      end
      local dirs = { }
      for fname in lfs.dir(ndir) do
        table.insert(dirs, fname)
      end
      table.sort(dirs)
      return dirs
    end,
    _relpath = function(self, dir, fname)
      local fullpath = path.fullpath(fname)
      print("fullpath = " .. tostring(fullpath))
      local s, e = string.find(fullpath, dir, 1, true)
      local res = string.sub(fullpath, e + 2)
      print("res = " .. tostring(res))
      return res
    end,
    list_files = function(self, dir)
      local fnames = self:_list_files_sorted(dir)
      self.logger:debug("list_files:\n" .. inspect(fnames))
      local result = { }
      for i, fname in ipairs(fnames) do
        local index, name = string.match(fname, "(%d+)_(.*).lua")
        if index and name and path.isfile(path.join(dir, fname)) then
          table.insert(result, fname)
        end
      end
      return result
    end,
    migrate_dir = function(self, dir, one)
      local ok, err = self:bootstrap()
      if not ok then
        print("Unable to bootstrap database: " .. tostring(err))
      end
      local fnames = self:_list_files_sorted(dir)
      for i, fname in ipairs(fnames) do
        local index, name = string.match(fname, "(%d+)_(.*).lua")
        if index and name then
          local relname = path.join(dir, fname)
          if path.isfile(relname) then
            local res
            res, err = self:migrate_one({
              [index] = dofile(relname)
            }, name)
            if not res then
              io.stderr:write('Exiting\n')
              break
            end
            if one then
              break
            end
          end
        end
      end
    end,
    migrate_one = function(self, migration, name)
      for index, updown in pairs(migration) do
        local mtime, err = self:has_migration(index)
        if not mtime or err then
          io.stderr:write("[" .. tostring(index) .. ":" .. tostring(name) .. "] ")
          local res
          res, err = self:migration_up(updown, index, name)
          if res then
            io.stderr:write('OK\n')
          else
            io.stderr:write("ERROR: " .. tostring(err) .. "\n")
          end
          return res, err
        else
          log.debug("[" .. tostring(index) .. ":" .. tostring(name) .. "] skipped")
          return mtime, err
        end
      end
    end,
    list = function(self)
      return self.tar:select("migrations", "primary")
    end,
    revert = function(self, dir)
      local res, err = self.meta.box.space.migrations.index.time:max()
      if err then
        return nil, err
      end
      local lastname = res[1]
      if lastname == "1" then
        return true, nil
      end
      local fnames = self:list_files(dir or "migrate")
      for i, fname in ipairs(fnames) do
        local index, name = string.match(fname, "(%d+)_(.*).lua")
        if lastname == index then
          self:migrate_down(lastname, dofile(fname))
        end
      end
    end
  }
  _base_0.__index = _base_0
  _class_0 = setmetatable({
    __init = function(self, args, logger)
      local err
      self.tar, err = tarantool(args)
      if self.tar.err then
        self.err = self.tar.err
        return self.tar, err
      end
      self.meta = _addmethod({
        obj = self.tar
      }, "")
      self.bootstrapped = false
      self.logger = logger or logging.new(self._appender)
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
  self.__class.bootstrap_migration = {
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
  self.__class.lua_template = "-- migration created by migrate.moon\nreturn {\n  up = function(migrator) -- forward migration\n  end,\n  down = function(migrator) -- backward migration\n  end\n}\n  "
  self.__class.moon_template = "-- migration created by migrate.moon\n{\n  up: (migrator) ->\n    -- forward migration\n  down: (migrator) ->\n    -- backward migration\n}\n"
  _appender = function(obj, level, msg)
    io.write(tostring(level) .. " " .. tostring(string.rep(' ', 5 - #level)))
    return io.write(tostring(msg) .. "\n")
  end
  Migrator = _class_0
end
return Migrator
