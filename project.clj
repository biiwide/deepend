(defproject biiwide/deepend "0.0.1"

  :description "Idiomatic object pools in Clojure."

  :url "http://github.com/biiwide/deepend"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [io.aleph/dirigiste "0.1.5"]]

  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]
                                  [proto-repl "0.3.1"]]}}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]
  )
                                  
