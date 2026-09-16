module Leveret
  # Subscribes to one or more queues and forks workers to perform jobs as they arrive
  #
  # Call {#do_work} to subscribe to all queues and block the main thread.
  class Worker
    extend Forwardable

    # Wall-clock bound on Configuration#before_child_exit. Generous enough for a log shipper to
    # complete one synchronous HTTP delivery, short enough that an unreachable sink delays a
    # child's exit by seconds rather than pinning a fork slot.
    CHILD_EXIT_HOOK_TIMEOUT = 5

    # @!attribute queues
    #   @return [Array<Queue>] All of the queues this worker is going to subscribe to
    # @!attribute consumers
    #   @return [Array<Bunny::Consumer>] All of the actively subscribed queues
    attr_accessor :queues, :consumers

    def_delegators :Leveret, :log, :configuration

    # Create a new worker to process jobs from the list of queue names passed
    #
    # @param [Array<String>] queue_names ([Leveret.configuration.default_queue_name]) A list of queue names for this
    #   worker to subscribe to and process
    def initialize(*queue_names)
      queue_names << configuration.default_queue_name if queue_names.empty?

      self.queues = queue_names.map { |name| Leveret::Queue.new(name) }
      self.consumers = []
      @time_to_die = false
    end

    # Subscribe to all of the {#queues} and begin processing jobs from them. This will block the main
    # thread until an interrupt is received.
    def do_work
      log.info "Starting master process for #{queues.map(&:name).join(', ')}"
      prepare_for_work

      loop do
        if @time_to_die
          cancel_subscriptions
          break
        end
        sleep 1
      end
      log.info "Exiting master process"
    end

    private

    # Steps that need to be prepared before we can begin processing jobs
    def prepare_for_work
      setup_traps
      self.process_name = 'leveret-worker-parent'
      start_subscriptions
    end

    # Catch INT and TERM signals and set an instance variable to signal the main loop to quit when possible
    def setup_traps
      trap('INT') do
        @time_to_die = true
      end
      trap('TERM') do
        @time_to_die = true
      end
    end

    # Set the title of this process so it's easier on the eye in top
    def process_name=(name)
      Process.setproctitle(name)
    end

    # Subscribe to each queue defined in {#queues} and add the returned consumer to {#consumers}. This will
    # allow us to gracefully cancel these subscriptions when we need to quit.
    def start_subscriptions
      queues.map do |queue|
        consumers << queue.subscribe do |incoming_message|
          fork_and_run(incoming_message)
        end
      end
    end

    # Send cancel to each consumer in the {#consumers} list. This will end the current subscription.
    def cancel_subscriptions
      log.info "Interrupt received, preparing to exit"
      consumers.each do |consumer|
        log.debug "Cancelling consumer on #{consumer.queue.name}"
        consumer.cancel
      end
    end

    # Fork the current process and run the job described by #payload in the newly created child process.
    # Detach the main process from the child so we can return to the main loop without waiting for it to finish
    # processing the job.
    #
    # @param [Message] payload Message meta and payload to process
    def fork_and_run(incoming_message)
      pid = fork do
        self.process_name = 'leveret-worker-child'
        log.info "[#{incoming_message.delivery_tag}] Forked to child process #{pid} to run" \
          "#{incoming_message.params[:job]}"

        Leveret.reset_connection!
        Leveret.configuration.after_fork.call

        result = perform_job(incoming_message.params)
        result_handler = Leveret::ResultHandler.new(incoming_message)
        result_handler.handle(result)

        log.info "[#{incoming_message.delivery_tag}] Exiting child process #{pid}"
        run_before_child_exit_hook
        flush_own_log
        exit!(0)
      end

      # Master doesn't need to know how it all went down, the worker will report it's own status back to the queue
      Process.detach(pid)
    end

    # Give the host application a chance to flush anything it has buffered before the child
    # leaves via #exit!.
    #
    # #exit! is deliberate -- it skips at_exit handlers that were registered in the PARENT and
    # inherited across the fork, which must not run once per job. But it skips ALL of them, and
    # an in-memory buffer flushed by an at_exit handler goes with them. An HTTP log shipper is
    # the common case: a batching sink relies on `at_exit { close }` to deliver its tail, so the
    # LAST lines a job writes -- the ones saying whether it succeeded -- are the ones most
    # reliably lost. Long jobs hide this, because a periodic flush ships everything except the
    # final batch; short jobs can lose their entire output.
    #
    # Runs AFTER the acknowledgement, so a hook that hangs cannot cause redelivery. Bounded and
    # rescued for the same reason: an unreachable sink must never stop a child exiting, or forks
    # accumulate until the host runs out of processes. A failed flush costs log lines; a wedged
    # child costs the worker.
    def run_before_child_exit_hook
      Timeout.timeout(CHILD_EXIT_HOOK_TIMEOUT) { configuration.before_child_exit.call }
    rescue Exception => e # rubocop:disable Lint/RescueException
      # Timeout::Error is not a StandardError on older rubies, and this runs microseconds before
      # exit! -- there is nothing left to protect by letting anything propagate.
      log.warn "before_child_exit hook failed: #{e.class}: #{e.message}"
    end

    # Flush OUR OWN log before exit! discards it. This gem's log_file defaults to STDOUT, and a
    # redirected STDOUT is block-buffered, so `Job returned ...` and `Exiting child process ...`
    # -- both written in the child, microseconds before exit! -- are usually never written out.
    #
    # This is not theoretical. On one production host over two days the log held 4,769
    # "Forked to child" lines (written by the PARENT, which exits normally and flushes) against
    # 14 "Job returned" lines (written by the CHILD). 0.3%. The result of virtually every job
    # this gem has ever run was discarded by its own exit path, which makes the log useless for
    # the one question it is most often asked: did that job succeed?
    #
    # Must run LAST, so it also flushes whatever before_child_exit logged.
    def flush_own_log
      device = log.instance_variable_get(:@logdev)
      io = device.respond_to?(:dev) ? device.dev : nil
      io.flush if io.respond_to?(:flush)
    rescue Exception # rubocop:disable Lint/RescueException
      # Nothing can be reported here -- reporting is what just failed -- and exit! is next.
    end

    # Constantize the class name in the payload and execute the job with parameters
    #
    # @param [Parameters] payload The job name and parameters the job requires
    # @return [Symbol] :success, :reject or :requeue depending on how job execution went
    def perform_job(payload)
      job_klass = Object.const_get(payload[:job])
      job_klass.perform(Leveret::Parameters.new(payload[:params]))
    end
  end
end
