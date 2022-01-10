# require 'thread'
# require "concurrent-ruby-ext"

class Thread::Queue
  # Run the following block, and return a new queue which contains the results of the evaluation
  # 
  # Order not guaranteed with more than one thread
  def map(threads: 1, &block)
    Thread::Queue.new.tap do |result|
      threads.times do
        Thread.start do
          until closed?
            result.push block.call(self.pop)
          end
          result.close
        end
      end
    end
  end

  # Run the block for each item, until killed by signal
  def drain(threads: 1, &block)
    threads.times.map do
      Thread.start do
        block.call(pop) until closed?
      end
    end
  end

  # Runs the function on the input pipeline and adds the value to a batch which will be sent later
  # at an interval set by window_ms 
  def window(window_ms: 1000, &function)
    Thread::Queue.new.tap do |result_queue|
      window_sec = window_ms.to_f / 1000.0
      current_window = {}
      Thread.start do
        until closed?
          value = pop
          (current_window[function.call(value)] ||= []).push(value)
        end
      end

      Thread.start do
        until closed?
          last_window = current_window
          current_window = {}
          last_window.each do |k, v|
            result_queue.push([k, v])
          end
          sleep window_sec
        end

        result_queue.close
      end
    end
  end
end

pipeline = Thread::Queue.new

map_to_int = pipeline.map(&:to_i)

window = map_to_int.window(window_ms: 1000) do |msg|
  (msg % 2) == 0 ? "even" : "odd"
end

drain = window.drain do |(key, batch)|
  puts "#{$$}: #{key} => #{batch.inject(0, &:+)}"
end

Signal.trap("SIGINT") { pipeline.close }

until pipeline.closed?
  pipeline.push(ARGF.readline) rescue false
end

# sleep 0.1 until drain.map(&:join)
