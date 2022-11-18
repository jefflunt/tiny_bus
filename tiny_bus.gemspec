Gem::Specification.new do |s|
  s.name        = "tiny_bus"
  s.version     = "1.0.1"
  s.summary     = "a tiny pubsub message bus with almost no features"
  s.description = "want to have an in-memory message bus that takes hash-like objects and distributes them out to subscribers based on a 'topic' key, with logging to $stdout or a file, and absolutely nothing else? then this library is for you"
  s.authors     = ["Jeff Lunt"]
  s.email       = "jefflunt@gmail.com"
  s.files       = ["lib/tiny_bus.rb"]
  s.homepage    = "https://github.com/jefflunt/tiny_bus"
  s.license     = "MIT"
  s.add_runtime_dependency "tiny_log", [">= 0"]
end
