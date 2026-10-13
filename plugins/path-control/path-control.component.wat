(component
  (core module $guest
    (memory (export "memory") 1)
    (global $heap (mut i32) (i32.const 1024))
    ;; A deliberately tiny allocator: each instance serves exactly one invocation.
    ;; A request's total projected strings are capped at 8192 bytes by the host.
    (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
      (local $old i32)
      global.get $heap
      local.tee $old
      local.get 3
      i32.add
      global.set $heap
      local.get $old)
    (func (export "authorize")
      (param $method i32) (param $mlen i32)
      (param $path i32) (param $plen i32)
      (param $subject i32) (param $slen i32)
      (param $tenant i32) (param $tlen i32)
      (param $operation i32) (param $olen i32)
      (param $target i32) (param $nlen i32)
      (result i32)
      ;; Deny /admin and /admin/...; the host provides a canonical path.
      local.get $plen
      i32.const 6
      i32.ge_u
      if
        local.get $path
        i32.load
        i32.const 1835295023 ;; little-endian "/adm"
        i32.eq
        local.get $path
        i32.const 4
        i32.add
        i32.load16_u
        i32.const 28265 ;; little-endian "in"
        i32.eq
        i32.and
        if
          local.get $plen
          i32.const 6
          i32.eq
          if
            i32.const 403
            return
          end
          local.get $path
          i32.const 6
          i32.add
          i32.load8_u
          i32.const 47
          i32.eq
          if
            i32.const 403
            return
          end
        end
      end
      ;; Defense-in-depth: deny admin_preview even for an operator.
      local.get $nlen
      i32.const 13
      i32.eq
      if
        local.get $target
        i32.const 0
        i32.add
        i32.load
        i32.const 1768776801
        i32.eq
        local.get $target
        i32.const 4
        i32.add
        i32.load
        i32.const 1919967086
        i32.eq
        i32.and
        local.get $target
        i32.const 8
        i32.add
        i32.load
        i32.const 1701410405
        i32.eq
        i32.and
        local.get $target
        i32.const 12
        i32.add
        i32.load8_u
        i32.const 119
        i32.eq
        i32.and
        if
          i32.const 403
          return
        end
      end
      i32.const 0))
  (core instance $instance (instantiate $guest))
  (func (export "authorize")
    (param "method" string) (param "path" string)
    (param "subject" string) (param "tenant" string)
    (param "operation" string) (param "target" string)
    (result u16)
    (canon lift (core func $instance "authorize")
      (memory (core memory $instance "memory")) (realloc (core func $instance "cabi_realloc")))))
