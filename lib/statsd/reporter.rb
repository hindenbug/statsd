require "thread"

module Statsd
  class Reporter

    attr_accessor :queue, :messages
    attr_reader :statsd_host, :batch_size, :pool_size

    def initialize(statsd_host, batch_size, pool_size)
      @pool_size   = pool_size || 1
      @statsd_host = statsd_host
      @messages    = []
      @queue       = Queue.new
      @batch_size  = batch_size
      @pool_size.times { |i| Thread.new { Thread.current[:id] = i; run! } }
    end

    def enqueue(metric)
      queue << metric
    end

    private

    def run!
      loop do
        if queue.size >= batch_size
          begin
            while messages << queue.pop(true)
              flush
            end
          rescue ThreadError
            flush
          end
        end
      end
    end

    def flush
      statsd_host.send_to_socket messages.join("\n")
      messages.clear
    end

  end
end
