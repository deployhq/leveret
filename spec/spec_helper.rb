$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'leveret'
# Three spec files build doubles with OpenStruct. ostruct is no longer loaded implicitly, so
# without this every example in those files errors before it runs -- 14 of them.
require 'ostruct'

Dir[File.join(File.dirname(__FILE__), 'support/**/*.rb')].each { |f| require f }

RSpec.configure do |c|
  c.include QueueHelpers

  c.before(:all) do
    Leveret.configure do |conf|
      # Overridable so CI can point at a broker that does not accept the loopback-only
      # `guest` account. Defaults to the same local broker developers already use.
      conf.amqp = ENV.fetch('LEVERET_AMQP_URL', 'amqp://guest:guest@localhost:5672')
      conf.log_level = Logger::ERROR
      conf.queue_name_prefix = 'leveret_test_queue'
      conf.default_queue_name = 'test'
      conf.exchange_name = 'leveret_test_exch'
      conf.delay_exchange_name = 'leveret_test_delay_exchange'
      conf.delay_queue_name = 'leveret_test_delay_queue'
    end

    flush_queue('test')
    flush_queue('other')
  end

  c.after(:each) do
    flush_queue('test')
    flush_queue('other')
  end
end
