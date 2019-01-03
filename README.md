# biiwide/deepend

Idiomatic connection pools in Clojure.

Based on [Dirigiste](https://github.com/ztellman/dirigiste).

[![Clojars Project](https://img.shields.io/clojars/v/biiwide/deepend.svg)](https://clojars.org/biiwide/deepend)

## Usage
```
(require [biiwide.deepend.alpha.pool :as pool])
```

Define a generator:
```
(pool/defgenerator some-connector
  [hostname port]
  (generate [k] (client/connection hostname port :credentials k))
  (destroy  [k conn] (client/close conn)))

(def prod-client (some-connector prod-host prod-port))
```

A pool can be constructed using `:generate` & `:destroy` functions, or
with a `:generator`.

Fixed object pool with `generate` & `destroy` functions:
```
(pool/pool
  {:generate    (fn [key] ...)
   :destroy     (fn [key obj] ...)
   :max-objects 10
   })
```

Fixed object pool with `generator`:
```
(pool/pool
  {:generator prod-client
   :max-objects-per-key 8
   :max-objects         24
   })
```

Utilization pool:
```
(pool/pool
  {:generator           my-custom-generator
   :target-utilization  1.0
   :max-objects-per-key 5
   :max-objects         15})
```

Pool with basic resource health check:
```
(pool/pool
  {:generator            the-generator
   :max-objects          8
   :healthy?             (fn [k obj] (alive? obj))
   :check-on-acquire     true
   :check-on-release     true
   :max-acquire-attempts 2})
```

Acquire, Release, & Dispose of objects:
```
(pool/acquire! pool key)
(pool/release! pool key obj)
(poll/dispose! pool key obj)
```

Macro form for managing pooled objects:
```
(pool/with-resource [my-obj my-pool some-key]
  (do-stuff-with my-obj))
```


## License

Copyright © 2018 Ted Cushman

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
