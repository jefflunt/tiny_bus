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
  LOGGING_LEVELS = {
    'sent' => 'SENT',
    'dead' => 'DEAD'
  }.freeze

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
  # logging_overrides:
  #   default: LOGGING_LEVELS
  #   allows you to specify a Hash of overrides of the logging levels for
  #   'SENT' and 'DEAD' message delivery; useful if the consuming application
  #   wants to use its own logging levels, or you just want the labels changed
  #   for some reason
  def initialize(log: nil, dead: nil, translator: nil, raise_on_dead: false,
                 annotation_prefix: ANNOTATION_PREFIX_DEFAULT,
                 logging_overrides: LOGGING_LEVELS)
    @subs = {}
    @translator = translator

    @dead_key = "#{annotation_prefix}dead"
    @topic_key = "#{annotation_prefix}topic"
    @time_key = "#{annotation_prefix}time"
    @msg_uuid_key = "#{annotation_prefix}msg_uuid"
    @trace_key = "#{annotation_prefix}trace"

    @annotator = TinyPipe.new([
      ->(m){ m[@time_key] = (Time.now.to_f * 1000).to_i; m },
      ->(m){ m[@msg_uuid_key] = SecureRandom.uuid; m },
      ->(m){ m[@trace_key] ||= SecureRandom.uuid; m }
    ])

    @stats = { '.dead' => 0 }
    @log = log || TinyLog.new($stdout)
    @dead = dead || TinyLog.new($stderr)
    @raise_on_dead = raise_on_dead
    @sent_level = logging_overrides['sent'] || LOGGING_LEVELS['sent']
    @dead_level = logging_overrides['dead'] || LOGGING_LEVELS['dead']
  end

  # adds a subscriber to a topic
  def sub(topic, subber)
    raise TinyBus::SubscriberDoesNotMsg.new("The specified subscriber type `#{subber.class.inspect}' does not respond to #msg") unless subber.respond_to?(:msg)

    @subs[topic] ||= Set.new
    @subs[topic] << subber
    @stats[topic] ||= 0

    msg({ @topic_key => 'sub', 'to_topic' => topic, 'subber' => subber.to_s })
  end

  # removes a subscriber from a topic
  def unsub(topic, subber)
    @subs[topic]&.delete(subber)

    msg({ @topic_key => 'unsub', 'from_topic' => topic, 'subber' => subber.to_s })
  end

  # takes an incoming message and distributes it to subscribers
  #
  # msg: the incoming message to be distributed
  # lvl (optional): the logging level
  #
  # NOTE: it modifies the incoming msg object in place in order to avoid
  # unnecessary object allocations
  #
  # NOTE: keys that begin with dot (.), such as '.time' are reserved for
  # TinyBus and show not be altered by outside code, otherwise undefined
  # behavior may result.
  def msg(msg, lvl='info')
    msg = @annotator.pipe(msg)
    msg = @translator&.pipe(msg) || msg

    topic = msg[@topic_key]

    subbers = @subs[topic]

    if (subbers&.length || 0) > 0
      @stats[topic] += 1
      subbers.each{|s| s.msg(msg) }
      @log.send(@sent_level, msg)
    else
      if @raise_on_dead
        raise TinyBus::DeadMsgError.new("Could not deliver message to topic `#{topic}'")
      else
        @stats[@dead_key] += 1
        @dead.send(@dead_level, msg)
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
class TinyBus::SubscriberDoesNotMsg < RuntimeError; end
