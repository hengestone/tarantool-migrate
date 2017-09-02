-- migrate.moon
tarantool = require "tarantool"
os = require "os"
lfs = require "lfs"
path = require "path"
inspect = require "inspect"

local _addmethod

_exec = (self, proc, ...) ->
  if type(proc) == "table" and proc.obj == self.obj
    self.obj.call(self.obj, "method", {self._method, proc._method, ...})
  else
    self.obj.call(self.obj, "meta", {self._method, proc, ...})

_addmethod = (self, _method) ->
  local obj
  if not self._method
    obj = self
    obj._method = _method
  else
    obj = {obj: self.obj}
    if #self._method == 0
      obj._method = _method
    else
      obj._method = self._method .. "." .. _method
  setmetatable(obj, { __index: _addmethod,  __call: _exec})
  obj

class Migrator
  @@bootstrap_migration = {
    up: (migrator) ->
      migrator.meta.box.schema.space.create('migrations')
      migrator.meta.box.space.migrations\create_index('primary', {if_not_exists: true, unique: true, type: 'TREE', parts: {1, 'string'}})
      migrator.meta.box.space.migrations\create_index('time', {if_not_exists: true, unique: true, type: 'TREE', parts: {2, 'unsigned'}})
    down: (migrator) ->
      migrator.meta.box.space.migrations\drop()
  }

  @@lua_template = "-- migration created by migrate.moon
return {
  up = function(migrator) -- forward migration
  end,
  down = function(migrator) -- backward migration
  end
}
  "

  @@moon_template = "-- migration created by migrate.moon
{
  up: (migrator) ->
    -- forward migration
  down: (migrator) ->
    -- backward migration
}
"

  new: (args) =>
    @tar, err = tarantool(args)
    if @tar.err
      @err = @tar.err
      return @tar, err
    @meta = _addmethod({obj: @tar}, "")
    @bootstrapped = false

  migration_up: (migration, index, name) =>
    tar\disable_lookups()
    res, err = migration.up(self)
    tar\enable_lookups()
    if res
      t = os.time()
      res, err = @tar\insert("migrations", {index, t, name})
      return not err, err
    else
      return nil, "migration_up #{index} failed: #{err}"

  migration_down: (migration, index) =>
    tar\disable_lookups()
    res, err = migration.down(self)
    tar\enable_lookups()
    if res
      res, err = @tar\delete("migrations", index)
      return not err, err
    else
      return nil, "migration_down #{index} failed: #{err}"

  has_migration: (index) =>
    res, err = @tar\select("migrations", "primary", index)
    return res[2], err

  check_bootstrapped: () =>
    res, err = @tar\select("_space", "name", "migrations")
    not (not res or #res == 0) and not err, err

  bootstrap: () =>
    if @bootstrapped
      return true, nil
    @bootstrapped, err = @check_bootstrapped()
    if not @bootstrapped
      @migration_up(@@bootstrap_migration, "1")
      @bootstrapped, err = @check_bootstrapped()
    @bootstrapped, err

  load_from_file: (fname) =>
    func, err = loadfile(fname)
    if err
      return nil, err
    nil

  open_or_create: (dir) =>
    info, err = lfs.attributes(dir)
    if not info
      lfs.mkdir(dir)
      info, err = lfs.attributes(dir)
    info.mode == "directory", err

  create_raw: (dir, name, template, ext) =>
    exist, err = @open_or_create(dir)
    if not exist
      return nil, err

    fname = path.join(dir, "#{os.date('%Y%m%d%H%M%S')}_#{name or 'migration'}.#{ext}")
    fh, err = io.open(fname, "w")
    if not fh or err
      return nil, err
    fh\write(template)
    fh\close()

  create_moon: (dir, name) =>
    @create_raw(dir or "migrate", name, @@moon_template, "moon")

  create_lua: (dir, name) =>
    @create_raw(dir or "migrate", name, @@lua_template, "lua")

  _list_files_sorted: (dir) =>
    ndir = path.normalize(dir)
    fullpath, err = path.isdir(ndir)
    if err or not fullpath
      return nil, "No such directory #{ndir}"

    dirs = {}
    for fname in lfs.dir(ndir)
      table.insert(dirs, fname)
    table.sort(dirs)
    return dirs

  _relpath: (dir, fname) =>
    fullpath = path.fullpath(fname)
    print("fullpath = #{fullpath}")
    s, e = string.find(fullpath, dir, 1, true)
    res = string.sub(fullpath, e+2)
    print("res = #{res}")
    res

  list_files: (dir) =>
    fnames = @_list_files_sorted(dir)
    result = {}
    for i, fname in ipairs(fnames)
      index, name = string.match(fname, "(%d+)_(.*).lua")
      if index and name and path.isfile(fname)
        table.insert(result, fname)
    result

  migrate_dir: (dir) =>
    ok, err = @bootstrap()
    if not ok
      print("Unable to bootstrap database: #{err}")

    fnames = @_list_files_sorted(dir)
    for i, fname in ipairs(fnames)
      index, name = string.match(fname, "(%d+)_(.*).lua")
      if index and name
        relname = path.join(dir, fname)
        if path.isfile(relname)
          res, err = @migrate_one({[index]: dofile(relname)}, name)
          if not res
            io.stderr\write('Exiting\n')
            break

  migrate_one: (migration, name) =>
    for index, updown in pairs(migration)
      mtime, err = @has_migration(index)
      if not mtime or err
        io.stderr\write("[#{index}:#{name}] ")
        res, err = @migration_up(updown, index, name)
        if res
          io.stderr\write('OK\n')
        else
          io.stderr\write("ERROR: #{err}\n")
        return res, err
      else
        log.debug("[#{index}:#{name}] skipped")
        return mtime, err

  list: () =>
    m.tar\select("migrations", "primary")

  revert: (dir) =>
    res, err = m.meta.box.space.migrations.index.time\max()
    if err
      return nil, err
    lastname = res[1]
    if lastname == "1" --  the bootstrap migration
      return true, nil

    fnames = _list_files(dir or "migrate")
    for i, fname in ipairs(fnames)
      index, name = string.match(fname, "(%d+)_(.*).lua")
      if lastname == index
        @migrate_down(lastname, dofile(fname))

Migrator