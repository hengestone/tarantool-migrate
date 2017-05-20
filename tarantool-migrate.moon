-- migrate.moon
tarantool = require "tarantool"
os = require "os"

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
  @bootstrap_migration = {
    up: (migrator) ->
      migrator.meta.box.schema.space.create('migrations')
      migrator.meta.box.space.migrations\create_index('primary', {if_not_exists: true, unique: true, type: 'TREE', parts: {1, 'string'}})
      migrator.meta.box.space.migrations\create_index('time', {if_not_exists: true, unique: true, type: 'TREE', parts: {2, 'unsigned'}})
    down: (migrator) ->
      migrator.meta.box.space.migrations\drop()
  }

  @lua_template = "-- migration created by migrate.moon
  return {
    up = function(migrator) -- forward migration
    end,
    down = function(migrator) -- backward migration
    end
  }
  "

  @moon_template = "-- migration created by migrate.moon
  {
    up: (migrator) ->
      -- forward migration
    down: (migrator) ->
      -- backward migration
  }
  "
  new: (args) =>
    @tar, err = tarantool\new(args)
    @meta = _addmethod({obj: @tar}, "")

  migration_up: (migrations, index) =>
    if migrations[index].up(self)
      res, err = @tar\insert("migrations", {index, os.time()})
      return not err, err
    else
      return nil, "migration_up #{index} failed"

  migration_down: (migrations, index) =>
    if migrations[index].down(self)
      @tar\delete("migrations", index)
      return true
    else
      return nil, "migration_down #{index} failed"

  has_migration: (index) =>
    res, err = @tar\select("migrations", "primary", index)
    not err

  bootstrap: () =>
    res, err = @tar\select("_space", "name", "migrations")
    if err
      migration_up(@bootstrap_migration, "1")

  load_from_file: (fname) =>
    func, err = loadfile(fname)
    if err
      return nil, err
    nil

  create_raw: (dir, name, template, ext) =>
    fname = fio.pathjoin(dir,"#{os.date('%Y%m%d%H%M%S')}_#{name or 'migration'}.#{ext}")
    fh, err = io.open(fname, "w")
    if not fh or err
      return nil, err
    fh\write(template)
    fh\close()

  migrate_dir: (dir) =>
    info = fio.stat(dir)
    if not info or not (info\is_dir())
      return nil, "No such directory #{dir}"
    io.stderr\write("processing migrations in #{dir}\n")
    fnames = fio.glob("#{dir}/*.lua")
    table.sort(fnames)
    for _, fname in ipairs(fnames)
      io.stderr\write("opening file #{fname}\n")
      index = string.match(fio.basename(fname), "(%d+)_.*%.lua")
      io.stderr\write("processing migration [#{index}]")
      do_migrations({[index]: dofile(fname)})

  do_migrations: (migrations) =>
    bootstrap(migrations)
    for index, updown in ipairs(migrations)
      if not has_migration(index)
        migrate_up(migrations, index)
      else
        log.info("skipping migration [#{index}]")
        true

Migrator