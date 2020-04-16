Gem::Specification.new do |s|
  s.name        = 'dataoperations-aggregate'
  s.version     = '0.0.1'
  s.date        = '2020-04-12'
  s.summary     = "Aggregate data"
  s.description = "Aggregate data over time"
  s.authors     = ["Victor Guillen"]
  s.email       = 'vguillen_public@gmail.com'
  s.files       = ["lib/aggregate.rb"]
  s.homepage    = 'https://github.com/superguillen/dataoperations-aggregate'
  s.license       = 'MIT'
  s.add_runtime_dependency 'descriptive_statistics'
end