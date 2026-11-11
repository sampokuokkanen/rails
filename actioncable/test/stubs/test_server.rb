# frozen_string_literal: true

require "delegate"
require "concurrent/atomic/atomic_fixnum"

# Runs tasks on Concurrent.global_io_executor and counts the ones that haven't
# finished, so wait_for_async knows when the test server's work is done.
class TestExecutor < SimpleDelegator
  PENDING = Concurrent::AtomicFixnum.new

  def self.idle? = PENDING.value.zero?

  def initialize
    super(Concurrent.global_io_executor)
  end

  def post(*args, &task)
    PENDING.increment
    __getobj__.post(*args) do |*task_args|
      task.call(*task_args)
    ensure
      PENDING.decrement
    end
  rescue Concurrent::RejectedExecutionError
    PENDING.decrement
    raise
  end

  def <<(task) = post(&task)
end

class TestServer < ActionCable::Server::Base
  class FakeConfiguration < ActionCable::Server::Configuration
    attr_accessor :subscription_adapter, :log_tags, :filter_parameters, :connection_class

    def initialize(subscription_adapter:)
      @log_tags = []
      @filter_parameters = []
      @subscription_adapter = subscription_adapter
      @connection_class = -> { ActionCable::Connection::Base }
    end

    def pubsub_adapter
      @subscription_adapter
    end
  end

  attr_reader :logger

  def initialize(subscription_adapter: SuccessAdapter)
    @logger = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new(%w[1 t true].include?(ENV["LOG"]) ? STDOUT : StringIO.new))
    @config = FakeConfiguration.new(subscription_adapter: subscription_adapter)
    @mutex = Monitor.new
  end

  def pubsub
    @pubsub ||= @config.subscription_adapter.new(self)
  end

  def event_loop
    @event_loop ||= ActionCable::Server::StreamEventLoop.new
  end

  def executor
    @executor ||= ActionCable::Server::ThreadedExecutor.new.tap do |ex|
      ex.instance_variable_set(:@executor, TestExecutor.new)
    end
  end

  def worker_pool
    @worker_pool ||= ActionCable::Server::Worker.new(max_size: 5).tap do |wp|
      wp.instance_variable_set(:@executor, TestExecutor.new)
    end
  end

  def new_tagged_logger = ActionCable::Server::TaggedLoggerProxy.new(logger, tags: [])
end
