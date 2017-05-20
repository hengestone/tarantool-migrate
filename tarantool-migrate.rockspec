package = "tarantool-migrate"
version = "scm-1"

source = {
  url = "git://github.com/hengestone/tarantool-migrate.git",
  branch = "master"
}

description = {
  summary = "Migration helper for Tarantool",
  homepage = "https://github.com/hengestone/tarantool-migrate",
  maintainer = "Conrad Steenberg <conrad.steenberg@gmail.com>",
  license = "MPLv2"
}

dependencies = {
  "lua ~> 5.1",
  "tarantool-lua"
}

build = {
  type = "builtin",
  modules = {
    ["tarantool-migrate"] = "tarantool-migrate.lua",
  },
  install = {
    lua = {
      ["tarantool-migrate"] = "tarantool-migrate.lua",
    }
  }
}

