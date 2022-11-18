require 'time'
require 'set'
require 'securerandom'
require 'tiny_log'

# This class implements a very simpler PubSub system where:
# - subscribers can subscribe via the #sub method
# - subscribers can unsubscribe via the #unsub method
# - msgs can enter the TinyBus via the #msg method
#
# The messages that come into this TinyBus are assumed to be Hash-like, in the
# sense that they have a 'topic' key that can be accessed using Hash-like key
# access syntax, and that the 'topic' key will serve as the method of
# distribution.
#
# Usage:
#   t = TinyBus.new
#   t.sub('news', <some object that responds to #msg)
#   t.msg({'topic' => 'news', 'details' => 'Historic happenings!})     # goes to 'news' subscribers
#   t.msg({'topic' => 'whatever', 'details' => 'Historic happenings!}) # goes to dead letter output, or raises exception, depending on the configuration
#
# Initialization options:
#   TinyBus.new(log: <a filename for log output>)                 # will log all normal msgs in this file
#   TinyBus.new(dead: <a filename for dead message log output>)   # will log all undeliverable msgs in this file
#   TinyBus.new(raise_on_dead: true)                              # strict mode for undeliverable messages, defaults to false
class TinyBus
  # log:
  #   if specified it should be a valid filename
  #   if not specified will default to $stdout
  # dead:
  #   if specified it should be a valid filename for dead letter logging
  #   if not specified will default to $stderr
  # raise_on_dead:
  #   kind of a strict mode. if false, then messages with a topic with no
  #   subscribers will go to the dead file. if true, then messages with a topic
  #   with no subscribers will raise an exception.
  def initialize(log: nil, dead: nil, raise_on_dead: false)
    @subs = {}
    @stats = { '.dead' => 0 }
    @log = log ? TinyLog.new(log) : TinyLog.new($stdout)
    @dead = dead ? File.open(dead, 'a') : TinyLog.new($stderr)
    @raise_on_dead = raise_on_dead
  end

  # adds a subscriber to a topic
  #
  # topics can be any string that doesn't start with a dot (.) - dot topics are
  # reserved for internal TinyBus usage, such as:
  # - .log
  def sub(topic, subber)
    raise TinyBus::SubscriptionToDotTopicError.new("Cannot subscribe to dot topic `#{topic}', because those are reserved for internal use") if topic.start_with?('.')
    raise TinyBus::SubscriberDoesNotMsg.new("The specified subscriber type `#{subber.class.inspect}' does not respond to #msg") unless subber.respond_to?(:msg)

    @subs[topic] ||= Set.new
    @subs[topic] << subber
    @stats[topic] ||= 0
  end

  # removes a subscriber from a topic
  def unsub(topic, subber)
    @subs[topic]&.delete(subber)
  end

  # takes an incoming message and distributes it to subscribers
  #
  # this method also annotates incoming messages with two dot properties:
  # - .time: the current timestamp, accurate to the microsecond
  # - .msg_uuid: a UUID to uniquely identify this message
  #
  # NOTE: it modifies the incoming msg object in place in order to avoid
  # unnecessary object allocations
  def msg(msg, lvl='info')
    topic = msg['topic']

    raise TinyBus::SendToDotTopicError.new("Cannot send to dot topic `#{topic}', because those are reserved for internal use") if topic.start_with?('.')

    subbers = @subs[topic]

    annotated = msg.merge!({
                '.time' => Time.now.utc.iso8601(6),
                '.msg_uuid' => SecureRandom.uuid
              })

    if subbers
      @stats[topic] += 1
      subbers.each{|s| s.msg(annotated) }
      @log.send(lvl, annotated)
    else
      if @raise_on_dead
        raise TinyBus::DeadMsgError.new("Could not deliver message to topic `#{topic}'")
      else
        @stats['.dead'] += 1
        @dead.send(lvl, annotated)
      end
    end
  end

  # helpful for debugging, gives you a count of the number of messages sent to
  # each topic, including the .dead topic, which is where messages go where
  # there are no subscribes for a given topic
  def to_s
    <<~DEBUG
    TinyBus stats: #{@subs.keys.length > 0 ? "\n  " + @stats.keys.sort.map{|t| "#{t.rjust(12)}: #{@stats[t]}" }.join("\n  ") : '<NONE>'}
    DEBUG
  end
end

class TinyBus::DeadMsgError < RuntimeError; end
class TinyBus::SubscriptionToDotTopicError < RuntimeError; end
class TinyBus::SubscriberDoesNotMsg < RuntimeError; end
class TinyBus::SendToDotTopicError< RuntimeError; end
