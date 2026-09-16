require 'spec_helper'

describe Leveret::Worker do
  describe '.new' do
    it 'uses the default queue if none is specified' do
      worker = Leveret::Worker.new
      expect(worker.queues.map(&:name)).to eq([Leveret.configuration.default_queue_name])
    end

    it 'can use custom queue names' do
      queue_names = %w[test other]

      worker = Leveret::Worker.new(*queue_names)
      expect(worker.queues.map(&:name)).to eq(queue_names)
    end
  end

  # The child leaves via exit!, which skips at_exit and every buffer flushed by one. This hook is
  # the only opportunity a batching sink gets to deliver what the job just wrote.
  describe '#run_before_child_exit_hook' do
    subject(:worker) { Leveret::Worker.new }

    def run_hook
      worker.send(:run_before_child_exit_hook)
    end

    around do |example|
      original = Leveret.configuration.before_child_exit
      example.run
      Leveret.configuration.before_child_exit = original
    end

    it 'calls the configured hook' do
      called = false
      Leveret.configuration.before_child_exit = proc { called = true }

      run_hook

      expect(called).to be(true)
    end

    it 'is a no-op with the default hook' do
      expect { run_hook }.not_to raise_error
    end

    it 'swallows an exception raised by the hook' do
      Leveret.configuration.before_child_exit = proc { raise 'sink unavailable' }

      expect { run_hook }.not_to raise_error
    end

    # Timeout::Error does not descend from StandardError on the rubies this gem supports, so a
    # bare `rescue StandardError` would let it escape and stop the child exiting.
    it 'swallows a non-StandardError raised by the hook' do
      Leveret.configuration.before_child_exit = proc { raise Timeout::Error, 'too slow' }

      expect { run_hook }.not_to raise_error
    end

    it 'gives up on a hanging hook instead of blocking the exit forever' do
      stub_const("#{described_class}::CHILD_EXIT_HOOK_TIMEOUT", 0.1)
      Leveret.configuration.before_child_exit = proc { sleep 5 }

      started = Time.now
      expect { run_hook }.not_to raise_error

      expect(Time.now - started).to be < 2
    end
  end
end
