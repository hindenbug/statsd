require "thread"

module Statsd
  class Reporter

    attr_accessor :queue, :workers, :running, :messages
    attr_reader :statsd_host, :batch_size

    def initialize(host, batch_size)
      @queue       = Queue.new
      @workers     = []
      @messages    = []
      @statsd_host = host
      @batch_size  = batch_size
    end

    def enqueue(metric)
      puts "inside queue"
      workers << Thread.new do
        queue << metric
        flush if queue.size >= batch_size
      end
      finish
    end

    private

    def flush
      begin
        while messages << queue.pop(true)
          unless messages.empty?
            statsd_host.send_to_socket messages.join("\n")
            messages.clear
          end
        end
      rescue ThreadError
      end
    end

    def finish
      workers.each(&:join)
    end

    def kill
      workers.each(&:kill)
      puts "========> Killed!"
    end

  end
end
