require "thread"

module StatsdMetrics
  class Reporter

    attr_accessor :queue, :threads_num, :workers, :running, :messages
    attr_reader :statsd_host, :queue_size, :batch_size

    def initialize(host, queue_size, batch_size, threads=nil)
      @queue       = Queue.new
      @threads_num = threads || 2
      @workers     = []
      @messages    = []
      @statsd_host = host
      @batch_size  = batch_size
      @queue_size  = queue_size
      @mutex       = Mutex.new
    end

    def enqueue(metric)
      # workers = threads_num.times.map { process if queue.size >= queue_size }
      queue << metric
      process if queue.size >= queue_size
    end

    def process
      threads_num.times do
        workers << Thread.new do
          @mutex.synchronize { flush }
        end
      end
      finish
    end

    private

    def flush
      begin
        while messages << queue.pop(true)
          if messages.size >= batch_size
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
