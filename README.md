# biiwide/deepend

Idiomatic connection pools in Clojure.

Based on [Dirigiste](https://github.com/ztellman/dirigiste).

[![Clojars Project](https://img.shields.io/clojars/v/biiwide/deepend.svg)](https://clojars.org/biiwide/deepend)

## Usage
```
(require [biiwide.deepend.alpha.pool :as pool])
```

Fixed object pool with health check on acquire:
```
(-> (pool/fixed-pool
      {:generate (fn [key] ...)
       :destroy  (fn [key obj] ...)
       :max-objects-per-key 10
       :max-objects-total   10})
    (pool/with-check
      (fn healthy-object? [key obj])
      {:on-acquire true
       :on-release false}))
```

Utilization pool:
```
(pool/utilization-pool
  {:generate (fn [key] ...)
   :destroy  (fn [key obj] ...)
   :target-utilization 1.0
   :max-objects-per-key 5
   :max-objects-total 15})
```

## License

Copyright © 2018 Ted Cushman

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
