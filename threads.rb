require "statsd_metrics"
require "pry"
require "benchmark"


$statsd = StatsdMetrics::Statsd.new("127.0.0.1", 8125)

Benchmark.bm do |bm|
 bm.report  do
   5000000.times { $statsd.increment("sample2.count") }
 end
end
