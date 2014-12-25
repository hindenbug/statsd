require "statsd_metrics/version"
require "statsd_metrics/reporter"
require "socket"

module StatsdMetrics

  class Statsd

    attr_accessor :namespace, :queue_size, :batch_size

    # StatsD host. Defaults to 127.0.0.1.
    attr_accessor :host

    # StatsD port. Defaults to 8125.
    attr_accessor :port

    attr_accessor :queue_size

    class << self
      # Set to a standard logger instance to enable debug logging.
      attr_accessor :logger
    end

    def initialize(host='127.0.0.1', port=8125, queue_size=1000, batch_size=100)
      @socket    = UDPSocket.new
      @host      = host
      @port      = port
      @reporter  = StatsdMetrics::Reporter.new(self, queue_size, batch_size)
    end

    def increment(stat, sample_rate=1)
      metric = form_metric(stat, 1, :c, sample_rate)
      @reporter.enqueue(metric)
    end

    def decrement(stat, sample_rate=1)
      metric = form_metric(stat, -1, :c, sample_rate)
      @reporter.enqueue(metric)
    end

    # Sends an arbitary gauge value for the given stat to the statsd server.
    #
    # This is useful for recording things like available disk space,
    # memory usage, and the like, which have different semantics than
    # counters.
    #
    # @param [String] stat stat name.
    # @param [Numeric] gauge value.
    # @param [Numeric] sample_rate sample rate, 1 for always
    # @example Report the current user count:
    #   $statsd.gauge('user.count', User.count)
    def gauge(stat, value, sample_rate=1)
      metric = form_metric(stat, value, :g, sample_rate)
      @reporter.enqueue(metric)
    end

    # Sends a timing (in ms) for the given stat to the statsd server. The
    # sample_rate determines what percentage of the time this report is sent. The
    # statsd server then uses the sample_rate to correctly track the average
    # timing for the stat.
    #
    # @param [String] stat stat name
    # @param [Integer] ms timing in milliseconds
    # @param [Numeric] sample_rate sample rate, 1 for always
    def timing(stat, ms, sample_rate=1)
      metric = form_metric(stat, ms, :ms, sample_rate)
      @reporter.enqueue(metric)
    end

    # Reports execution time of the provided block using {#timing}.
    #
    # @param [String] stat stat name
    # @param [Numeric] sample_rate sample rate, 1 for always
    # @yield The operation to be timed
    # @see #timing
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

    private

    def form_metric(stat, delta, type, sample_rate=1)
      if sample_rate == 1 or rand < sample_rate
        # Replace Ruby module scoping with '.' and reserved chars (: | @) with underscores.
        stat = stat.to_s.gsub('::', '.').tr(':|@', '_')
        rate = "|@#{sample_rate}" unless sample_rate == 1
        return "#{namespace}.#{stat}:#{delta}|#{type}#{rate}"
      end
    end

  end

end
