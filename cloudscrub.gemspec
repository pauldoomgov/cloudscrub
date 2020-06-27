# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cloudscrub/version'

Gem::Specification.new do |spec|
  spec.name          = "cloudscrub"
  spec.version       = CloudScrub::VERSION
  spec.authors       = ["Login.gov"]
  spec.email         = ["hello@login.gov"]

  spec.summary       = "AWS CloudWatch log stream scrubbing tool"
  spec.description   = "When your application makes a mess cloudscrub will get your logs sparkling clean"
  spec.homepage      = "https://github.com/18f/cloudscrub"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.6.0")

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
end
