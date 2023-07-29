require 'set'
require 'time'
require 'set'
require 'securerandom'
require 'tiny_log'
require 'tiny_pipe'

# NOTE: This library depends on the TinyLog library.
#
# This class implements a very simple PubSub system where:
# - subscribers can subscribe via the #sub method
# - subscribers can unsubscribe via the #unsub method
# - msgs can enter the TinyBus via the #msg method
#
# The messages that come into this TinyBus are assumed to be Hash-like, in the
# sense that they have a '.topic' key that can be accessed using Hash-like key
# access syntax, and that the '.topic' key will serve as the method of
# distribution.
#
# Usage:
#   t = TinyBus.new
#   t.sub('news', <some object that responds to #msg)
#   t.msg({'.topic' => 'news', 'details' => 'Historic happenings!})     # goes to 'news' subscribers
#   t.msg({'.topic' => 'whatever', 'details' => 'Historic happenings!}) # goes to dead letter output, or raises exception, depending on the configuration
#
# Initialization options:
#   TinyBus.new(log: <a TinyLog for output>)           # will log all normal msgs in this file
#   TinyBus.new(dead: <a TinyLog for dead messages>)   # will log all undeliverable msgs in this file
#   TinyBus.new(raise_on_dead: true)                   # strict mode for undeliverable messages, defaults to false
class TinyBus
  ANNOTATION_PREFIX_DEFAULT = '.'

  attr_reader :dead_topics

  # log:
  #   if specified, it should be a TinyLog instance
  #   if not specified, it will create a new TinyLog instance for $stdout
  # dead:
  #   if specified, it should be a TinyLog instance
  #   if not specified, it will create a new TinyLog instance for $stderr
  # translator:
  #   the translator is an instance of TinyPipe, if you want to translate the
  #   incoming masssage (i.e. annotate with additional fields, change
  #   keys/values on incoming messges). if not specified no translatioins will
  #   be made on incoming messages other than the default annotations
  #   NOTE: all messages are automatically annotated with three fields:
  #   - .time: the Unix time the message is received in Integer milliseconds,
  #   - .msg_uuid: a unique UUID for the incoming message
  #   - .trace: a unique UUID for chains of messages (if not present)
  # raise_on_dead:
  #   kind of a strict mode. if false, then messages with a '.topic' with no
  #   subscribers will go to the dead file. if true, then messages with a
  #   '.topic' with no subscribers will raise an exception.
  # annotation_prefix:
  #   default: '.'
  #   if specified, the annotated message attributes ('.time', '.msg_uuid', and
  #   '.trace') will have the dot ('.') replaced with the specified prefix text
  def initialize(log: nil, dead: nil, translator: nil, raise_on_dead: false,
                 annotation_prefix: ANNOTATION_PREFIX_DEFAULT)
    @subs = {}
    @dead_topics = Set.new
    @translator = translator

    @total_key = "#{annotation_prefix}total"
    @dead_key = "#{annotation_prefix}dead"
    @topic_key = "#{annotation_prefix}topic"
    @time_key = "#{annotation_prefix}time"
    @msg_uuid_key = "#{annotation_prefix}msg_uuid"
    @trace_key = "#{annotation_prefix}trace"

    @annotator = TinyPipe.new([
      ->(m){ m[@time_key] ||= (Time.now.to_f * 1000).to_i; m },
      ->(m){ m[@msg_uuid_key] ||= SecureRandom.uuid; m },
      ->(m){ m[@trace_key] ||= SecureRandom.uuid; m }
    ])

    @stats = { @total_key => 0, @dead_key => 0 }
    @log = log || TinyLog.new(filename: $stdout)
    @dead = dead || TinyLog.new(filename: $stderr)
    @raise_on_dead = raise_on_dead
  end

  # adds a subscriber to a topic
  #
  # - topic: either a String, or an array of String
  # - subber: an object that responds to #msg that can receive messages
  def sub(topic, subber)
    raise TinyBus::SubscriberDoesNotMsg.new("The specified subscriber type `#{subber.class.inspect}' does not respond to #msg") unless subber.respond_to?(:msg)

    topics = topic.is_a?(Array) ? topic : [topic]

    topics.each do |t|
      @dead_topics.delete(t)
      @subs[t] ||= Set.new
      @subs[t] << subber
      @stats[t] ||= 0

      msg({ @topic_key => 'sub', 'to_topic' => t, 'subber' => _to_subber_id(subber) }, 'TINYBUS-SUB')
    end
  end

  # removes a subscriber from a topic
  def unsub(topic, subber)
    @subs[topic]&.delete(subber)
    @dead_topics << topic if @subs[topic].empty?

    msg({ @topic_key => 'unsub', 'from_topic' => topic, 'subber' => _to_subber_id(subber) }, 'TINYBUS-UNSUB')
  end

  def _to_subber_id(subber)
    "#{subber.class.name}@#{subber.object_id}"
  end

  # takes an incoming message and distributes it to subscribers
  #
  # msg: the incoming message to be distributed, must be a Ruby Hash
  # lvl (optional): the logging level
  #   default: 'info'
  #
  # NOTE: this method modifies the incoming msg object in place in order to
  # avoid unnecessary object allocations
  #
  # NOTE: keys that begin with dot (.), such as '.time' are reserved for
  # TinyBus and show not be altered by outside code, otherwise undefined
  # behavior may result.
  def msg(msg, lvl=:info)
    msg = @annotator.pipe(msg)
    msg = @translator&.pipe(msg) || msg

    topic = msg[@topic_key]
    subbers = @subs[topic]

    @stats[topic] ||= 0
    @stats[@total_key] += 1
    @stats[topic] += 1
    if (subbers&.length || 0) > 0
      # cloning is necessary, because sending messanges may result in new
      # subscribers to the same topic we're iterating over right now. in that
      # situation we would run into a RuntimeError that prevented the
      # modification of the Set we're iterating over to send messages.
      subbers.clone.each{|s| s.msg(msg) }

      @log.send(lvl, "S #{msg}")
    else
      if @raise_on_dead
        raise TinyBus::DeadMsgError.new("Could not deliver message to topic `#{topic}'")
      else
        @stats[@dead_key] += 1
        @dead_topics << topic
        @dead.send(lvl, "D #{msg}")
      end
    end
  end

  # returns a #dup of the internal statistics which track the number of
  # messages sent to each topic, the dead queue, and total messages
  def stats
    @stats.dup
  end

  # helpful for debugging, gives you a count of the number of messages sent to
  # each topic, including the .dead topic, which is where messages go where
  # there are no subscribes for a given topic
  def to_s
    <<~DEBUG
      TinyBus stats: #{@stats.keys.length > 0 ? "\n  " + @stats.keys.sort.map{|t| "#{t.rjust(12)}: #{@stats[t]}" }.join("\n  ") : '<NONE>'}
      Dead topics: [
        #{@dead_topics.sort.each_slice(3).map{|slice| slice.join(', ') }.join("\n  ")}
      ]
      Topics & Subscribers:
        #{@subs.map{|topic, subbers| "#{topic}:\n    #{subbers.map(&:to_s).join("\n    ")}" }.join("\n  ") }
    DEBUG
  end
end

class TinyBus::DeadMsgError < RuntimeError; end
class TinyBus::SubscriberDoesNotMsg < RuntimeError; end
