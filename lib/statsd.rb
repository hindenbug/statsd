require "statsd/version"
require "statsd/reporter"
require "socket"
require "logger"
require 'forwardable'

module Statsd

  class Base

    attr_accessor :namespace, :counter

    # StatsD host. Defaults to 127.0.0.1.
    attr_accessor :host

    # StatsD port. Defaults to 8125.
    attr_accessor :port

    class << self
      # Set to a standard logger instance to enable debug logging.
      attr_accessor :logger
    end

    def initialize(host='127.0.0.1', port=8125)
      @socket     = UDPSocket.new
      @host       = host
      @port       = port
      @counter    = 1
    end

    def increment(stat, sample_rate=1)
      count stat, 1, sample_rate
    end

    def decrement(stat, sample_rate=1)
      count stat, -1, sample_rate
    end

    def count(stat, count, sample_rate=1)
      send_stat(stat, count, :c, sample_rate)
    end

    # Sends an arbitary gauge value for the given stat to the statsd server.
    # This is useful for recording things like available disk space,
    # memory usage, and the like, which have different semantics than
    # counters.
    #
    # @example Report the current user count:
    #   $statsd.gauge('user.count', User.count)
    def gauge(stat, value, sample_rate=1)
      send_stat(stat, value, :g, sample_rate)
    end

    # Sends a timing (in ms) for the given stat to the statsd server. The
    # sample_rate determines what percentage of the time this report is sent. The
    # statsd server then uses the sample_rate to correctly track the average
    # timing for the stat.
    def timing(stat, ms, sample_rate=1)
      send_stat(stat, ms, :ms, sample_rate)
    end

    # Reports execution time of the provided block using {#timing}.
    #
    # @example Report the time (in ms) taken to activate an account
    #   $statsd.time('account.activate') { @account.activate! }
    def time(stat, sample_rate=1)
      start = Time.now
      result = yield
      timing(stat, ((Time.now - start) * 1000).round, sample_rate)
      result
    end

    def send_to_socket(message)
      self.class.logger.debug { "Statsd: #{message}" } if self.class.logger
      @socket.send(message, 0, @host, @port)
    rescue => boom
      self.class.logger.error { "Statsd: #{boom.class} #{boom}" } if self.class.logger
      nil
    end

    protected

    def send_stat(stat, delta, type, sample_rate=1)
      if sample_rate == 1 or rand < sample_rate
        stat   = stat.to_s.gsub('::', '.').tr(':|@', '_')
        prefix = "#{@namespace}." unless @namespace.nil?
        rate   = "|@#{sample_rate}" unless sample_rate == 1
        send_to_socket("#{prefix}#{stat}:#{delta}|#{type}#{rate}")
      end
    end
  end

  class Batch < Base

    extend Forwardable

    attr_accessor :batch_size, :pool_size
    def_delegators :@reporter, :spawn_thread_pool, :spawn_thread_pool

    def initialize(statsd, batch_size)
      @batch_size = batch_size
      @namespace  = statsd.namespace
      @reporter   = Statsd::Reporter.new(statsd, batch_size, pool_size)
    end

    protected

    def send_stat(stat, delta, type, sample_rate=1)
      if sample_rate == 1 or rand < sample_rate
        stat   = stat.to_s.gsub('::', '.').tr(':|@', '_')
        prefix = "#{@namespace}." unless @namespace.nil?
        rate   = "|@#{sample_rate}" unless sample_rate == 1
        msg    = "#{prefix}#{stat}:#{delta}|#{type}#{rate}"
        check_and_enqueue(msg)
      end
    end

    def check_and_enqueue(msg)
      if @reporter.queue.size == @reporter.batch_size
        logger = Logger.new('queue.log')
        logger.warn "Queue at Max Capacity !"
      else
        @reporter.enqueue(msg)
      end
    end
  end

end
