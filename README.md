# StatsdMetrics

TODO: Write a gem description

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'statsd-metrics'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install statsd-metrics

## Usage

```ruby
  #default host="127.0.0.1" and port=8125
  $statsd = Statsd::Base.new
  $statsd.increment("somestats")
  
  # using batching with queues
  # takes 2 params; A statsd instance and batch_size (defaults to 100)
  $batchd = Statsd::Batch.new($statsd, 100)
  
  #Makes use of ruby Queues internally
  $batchd.increment("somestats")
```


## Contributing

1. Fork it ( https://github.com/[my-github-username]/statsd-metrics/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
