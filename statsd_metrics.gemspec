# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require 'statsd_metrics/version'

Gem::Specification.new do |spec|
  spec.name          = "statsd_metrics"
  spec.version       = StatsdMetrics::VERSION
  spec.authors       = ["hindenbug"]
  spec.email         = ["manoj.mk27@gmail.com"]
  spec.summary       = "A small gem to enqueue statsd metrics and send them to the statsd/grpahite server over UDP"
  spec.description   = ""
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.1"
  spec.add_development_dependency 'pry', '~> 0.10.1'
end
