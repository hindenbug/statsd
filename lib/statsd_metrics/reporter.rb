require "thread"

module StatsdMetrics
  class Reporter

    attr_accessor :queue, :threads_num, :workers, :running
    attr_reader :statsd_host, :queue_size

    def initialize(host, queue_size, batch_size, threads=nil)
      @queue       = Queue.new
      @threads_num = threads || 1
      @workers     = []
      @messages    = []
      @statsd_host = host
      @batch_size  = batch_size
      @queue_size  = queue_size
    end

    def enqueue(metric, worker=StatsdJob)
      queue << Job.new(worker, metric)

      while queue.length > queue_size
        process
      end
    end

    def process
      workers = threads_num.times.map do
        Thread.new do
          messages << queue.pop
          while messages.size >= batch_size
            flush
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
      stop
    end

    def stop
      workers.each(&:join)
      puts "========> Complete!"
    end

    def kill
      workers.each(&:kill)
      puts "========> Killed!"
    end

  end

end

