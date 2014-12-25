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
      process if (queue << metric).size >= queue_size
      finish
    end

    def process
      workers = threads_num.times.map do |i|
        Thread.new do
          Thread.current[:id] = i
          @mutex.synchronize do
            while messages << queue.pop
              flush if messages.size >= batch_size
            end
          end
        end
      end
    end

    private

    def flush
      begin
        unless messages.empty?
          host.send_to_socket messages.join("\n")
          messages.clear
        end
      rescue ThreadError
      end
    end

    def finish
      workers.each(&:join)
      puts "========> Complete!"
    end

    def kill
      workers.each(&:kill)
      puts "========> Killed!"
    end

  end
end
