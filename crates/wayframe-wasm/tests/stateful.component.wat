(component
  (core module $guest
    (memory (export "memory") 1)
    (global $heap (mut i32) (i32.const 1024))
    (global $called (mut i32) (i32.const 0))
    (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
      (local $old i32)
      global.get $heap
      local.tee $old
      local.get 3
      i32.add
      global.set $heap
      local.get $old)
    (func (export "authorize") (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32) (result i32)
      global.get $called
      if (result i32)
        i32.const 403
      else
        i32.const 1
        global.set $called
        i32.const 0
      end))
  (core instance $instance (instantiate $guest))
  (func (export "authorize")
    (param "method" string) (param "path" string) (param "subject" string) (param "tenant" string)
    (param "operation" string) (param "target" string)
    (result u16)
    (canon lift (core func $instance "authorize")
      (memory (core memory $instance "memory")) (realloc (core func $instance "cabi_realloc")))))
