(ns biiwide.deepend.alpha.stats
  (:require [clojure.string :as str])
  (:import  [io.aleph.dirigiste Stats]))


(defn metrics
  [^Stats stats]
  (for [s (seq (.getMetrics stats))]
    (keyword (str/lower-case (.name s)))))


(definline num-workers
  [^Stats s]
  `(.getNumWorkers ~s))


(definline utilization
  [^Stats s quantile]
  `(.getUtilization ~s ~quantile))


(definline mean-utilization
  [^Stats s]
  `(.getMeanUtilization ~s))


(definline task-arrival-rate
  [^Stats s quantile]
  `(.getTaskArrivalRate ^Stats ~s ~quantile))


(definline mean-task-arrival-rate
  [^Stats s]
  `(.getMeanTaskArrivalRate ~s))


(definline task-completion-rate
  [^Stats s quantile]
  `(.getTaskCompletionRate ~s ~quantile))


(definline mean-task-completion-rate
  [^Stats s]
  `(.getMeanTaskCompletionRate ~s))


(definline task-rejection-rate
  [^Stats s quantile]
  `(.getTaskRejectionRate ~s ~quantile))


(definline mean-task-rejection-rate
  [^Stats s]
  `(.getMeanTaskRejectionRate ~s))


(definline queue-length
  [^Stats s quantile]
  `(.getQueueLength ~s ~quantile))


(definline mean-queue-length
  [^Stats s]
  `(.getMeanQueueLength ~s))


(definline queue-latency
  [^Stats s quantile]
  `(.getQueueLatency ~s ~quantile))


(definline mean-queue-latency
  [^Stats s]
  `(.getMeanQueueLatency ~s))

(definline task-latency
  [^Stats s quantile]
  `(.getTaskLatency ~s ~quantile))

(definline mean-task-latency
  [^Stats s]
  `(.getMeanTaskLatency ~s))
