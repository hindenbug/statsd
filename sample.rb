#require "statsd-ruby"
require "statsd"
require "pry"
require "benchmark"


$statsd = Statsd::Base.new("127.0.0.1", 8125)
$batch  = Statsd::Batch.new($statsd, 10)
#$statsd =  Statsd.new("127.0.0.1", 8125)

Benchmark.bm do |bm|
 bm.report  do
   1000000.times { $statsd.increment("qwewqeq.count") }
 end
end
