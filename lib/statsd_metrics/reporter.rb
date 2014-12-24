require "thread"

module StatsdMetrics
  class Reporter

    attr_accessor :queue, :threads_num, :workers, :running
    attr_reader :statsd_host, :queue_size

    def initialize(host, queue_size, threads=nil)
      @queue       = Queue.new
      @threads_num = threads || 1
      @workers     = []
      @running     = true
      @statsd_host = host
      @queue_size  = queue_size
    end

    Job = Struct.new(:worker, :metric)

    def enqueue(metric, worker=StatsdJob)
      queue << Job.new(worker, metric)

      while queue.length > queue_size
        process
      end
    end

    def process
      workers = threads_num.times.map do
        Thread.new { process_jobs }
      end
    end

    private

    def process_jobs
      begin
        empty! if queue.length > queue_size
      rescue ThreadError
      end
    end

    def empty!
      until queue.size.zero?
        job = queue.pop rescue nil
        job.worker.new.call(statsd_host, job.metric) if job
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

  class StatsdJob

    def call(host, message)
      host.send_to_socket(message)
    end
  end

end
