require 'date'
require 'logger'
require 'json'
require 'jsonpath'

#require 'aws-sdk'
require 'aws-sdk-cloudwatchlogs'
require 'aws-sdk-s3'


# Set of utilities for download, scrub, S3 stashing, and replacement of
# CloudWatch log streams
class CloudScrub
  
  # @param [Bool] cli_mode Set true for cli style logging
  # @param [Filehandle] log_to Filehandle to write to
  # @param [Logger:: LEVEL] log_level Logger log level to log at
  # @param [String] aws_log_file Debug log file for AWS calls
  # @param [Bool] dry_run Read but don't write to any AWS resource
  def initialize(cli_mode: false, log_to: STDERR, log_level: Logger::INFO, aws_log_file: nil, dry_run: false)
    @cli_mode = cli_mode
    @log_to = log_to
    @log_level = log_level
    @dry_run = dry_run

    if aws_log_file
      enable_aws_logging(aws_log_file)
    end
  end

  # @return [String] Dry run informative tag or empty string
  def dry_tag
    @dry_run ? ' [DRY-RUN]' : ''
  end

  def log
    # Return log, setting up appropriate log if not configured
    return @log if @log

    @log = Logger.new(@log_to)
    @log.level = @log_level

    if @cli_mode
      @log.progname = File.basename($0)
    else
      @log.progname = self.name
    end

    @log
  end

  # @param [String] aws_log_file File to capture AWS call logs in
  def enable_aws_logging(aws_log_file)
    # Create a modified short logger that does not add an extra \n
    aws_log_formatter = Aws::Log::Formatter.new(
      "[:client_class :http_response_status_code :time] :operation :error_class",
      max_string_size: 1000
    )
    # Setup default AWS logger - Defaults to INFO and seems to ignore other levels :(
    Aws.config.update({:logger => Logger.new(aws_log_file),
                       :log_formatter => aws_log_formatter})
  end

  # @return [Aws::CloudWatchLogs::Client] On-demand initialized CloudWatch
  #                                       client for reading (ignores dry_run)
  def cloudwatch_client_ro
    @cloudwatch_client_ro ||= Aws::CloudWatchLogs::Client.new
  end

  # @return [Aws::CloudWatchLogs::Client] On-demand initialized CloudWatch
  #                                       client for writing (honors dry_run)
  def cloudwatch_client_rw
    @cloudwatch_client_rw ||= Aws::CloudWatchLogs::Client.new(stub_responses: @dry_run)
  end

  # @return [Aws::S3::Client] On-demand initialized S3 client (honors dry_run)
  def s3_client
    @s3_client ||= Aws::S3::Client.new(stub_responses: @dry_run)
  end

  # @param [String] group_name CloudWatch log group
  # @param [Integer] start_time Start time in ms since epoch
  # @param [Integer] end_time End time in ms since epoch
  #
  # @return [List[Aws::CloudWatch::Stream]] List of CloudWatch::Stream objects
  def list_streams(group_name, start_time: nil, end_time: nil)
    streams = []
    cloudwatch_client_ro.describe_log_streams(
      order_by: 'LastEventTime', log_group_name: group_name
    ).each do |resp|
      streams.concat(resp.log_streams)
    end

    if streams.length == 0
      log.warn("No streams found in #{group_name.inspect} for time range")
      return streams
    end

    # if start time is provided, find all streams that have at least one event
    # after start_time
    if start_time
      streams.select! { |s|
        !s.last_event_timestamp.nil? && s.last_event_timestamp >= start_time
      }
    end

    # if end time is provided, find all streams that have at least one event
    # before end_time
    if end_time
      streams.select! { |s|
        !s.first_event_timestamp.nil? && s.first_event_timestamp <= end_time
      }
    end

    # make sure we have a deterministic order
    streams.sort_by!(&:log_stream_name)

    # See https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_LogStream.html
    # for a list of attributes included on each stream.  Note the storedBytes
    # attribute is deprecated so we have no idea how much data we are downloading.
    streams
  end

  # @param [String] message Log line to filter
  # @param [String or Regex] scrub_gsub String (for plain match) or regex to filter
  #
  # @return [String] Message with matching text removed
  def scrub_message_by_gsub(message, scrub_gsub)
    # Apply regular expressions to a line allowing arbitrary changes to the
    message.gsub(scrub_gsub, '')
  end

  # @param [String] message Event message
  # @param [Hash] jsonpaths Hash with keys of string JSON path
  #                         and values of matching JsonPath objects
  #
  # @return [String] Message with matching JSON elements removed
  def scrub_message_by_jsonpaths(message, jsonpaths)
    # Remove elements matching redact_paths from data, unless they match an
    # element in exclude_paths.  Use the jsonpath CLI tool to tune your
    # filters.
    dirty = false

    begin
      # Build JSON parsed version of message
      json_message = JsonPath.for(message)
    rescue JSON::ParserError, MultiJson::ParseError => err
      log.debug("JSON parser failure: #{err.inspect}")
      return message
    end

    jsonpaths.each do |path_string, path_obj|
      # Find then delete so we can avoid pointless reserialization
      unless path_obj.on(message).empty?
        dirty = true
        json_message = json_message.delete(path_string)
      end
    end

    if dirty
      message = json_message.to_hash.to_json(ascii_only: true)
    end

    message
  end

  # @param [Integer] timestamp ms since epoch
  # @param [String] stream_name Original stream name
  # @param [String] message Event message
  #
  # @return [String] LF terminated string ready to write to file
  #
  # Adds logstash-like timestamp and hostname to the input log event.
  def serialize_event(timestamp, stream_name, message)
    [timestamp, stream_name, message].join(' ') + "\n"
  end

  # @param [String] Line from saved log file
  #
  # @return [Array[Integer, String, String]] timestamp, stream_name, and raw log line
  def deserialize_event(logline)
    timestamp, stream_name, message = logline.chomp.split(' ', 3)
  end

  # @param [Integer] ms_timestamp Milliseconds since epoch
  #
  # @return [Time] Time object
  def parse_timestamp_ms(ms_timestamp)
    Time.at(ms_timestamp / 1000.0).utc
  end

  # @param [Time] time Time object
  #
  # @return [Integer] Milliseconds since epoch
  def time_to_stamp_ms(time)
    unless time.is_a?(Time)
      raise ArgumentError.new("Unexpected Time object: #{time.inspect}")
    end

    (time.to_f * 1000).round
  end

  # @param [String] timestring Time represented as seconds since epoch OR
  #                 ISO 8601 time string
  #
  # @return [Integer] Milliseconds since the epoch (as used in AWS)
  def timestring_to_stamp_ms(timestring)
    # WARNING - Uses Date which is very slow compared to Time!  Do
    # not use this in tight loops!

    # Epoch seconds
    if timestring.match(/^[\d\.]+$/)
      t = Time.at(timestring.to_f).utc
    else
      begin
        t = DateTime.parse(timestring).to_time.utc
      rescue ArgumentError
        raise ArgumentError.new("Could not parse #{timestring} as ISO8601 time")
      end
    end

    (t.to_f * 1000).round
  end

  # @param [String] group_name Log group to work in
  # @param [String] stream_name Complete stream to pull from
  #
  # @return [Hash] Next event in stream
  def each_event_in_stream(group_name, stream_name)
    unless block_given?
      return to_enum(:each_log_event_in_stream, stream_name, start_time:
                     start_time, end_time: end_time)
    end
    cloudwatch_client_ro.get_log_events(
      log_group_name: group_name,
      log_stream_name: stream_name,
      start_from_head: true,
      start_time: nil,
      end_time: nil
    ).each do |resp|
      resp.events.each do |event|
        yield event
      end
    end
  end

  # @param [String] group_name Name of log group to work in
  # @param [String] stream_name Name of stream to process
  # @param [String] local_dir Local directory to save stream files under
  # @param [Integer] split_bytes Maximum size of output text files that are saved
  #                              locally and saved in S3
  # @param [String or Regex] scrub_gsub gsub expression to filter from logs
  # @param [Array] scrub_jsonpaths List of JSONPath expressions to delete from logs
  # @return [Hash] A hash with the following items:
  #                - filenames - Array of saved files from the stream
  #                - event_count - Integer count of events in stream
  #                - scrubbed_count - Integer count of messages that were altered
  #                - in_bytes - Integer event bytes received from CloudWatch
  #                - out_bytes - Integer event bytes written (will be less than in
  #                  if elements have been scrubbed)
  def download_and_filter_stream(
    group_name,
    stream_name,
    local_dir: '.',
    split_bytes: nil,
    scrub_jsonpaths: nil,
    scrub_gsub: nil
  )
    # Why is this method all jumbled together?  We don't want PII to hit the
    # filesystem, so we need to scrub before writing.  HOWEVER we also need
    # to support splitting huge streams into file sets.  There.  I have justified
    # bad design.

    filenames = []
    event_count = 0
    scrubbed_count = 0
    in_bytes = 0
    out_bytes = 0
    next_file_index = 0
    cur_file = nil
    cur_file_bytes = nil

    # Build a map of path to JsonPath objects
    if !scrub_jsonpaths.nil?
      jsonpaths = scrub_jsonpaths.map { |p| [p, JsonPath.new(p)] }.to_h
    end

    log.info("Downloading log stream \"#{group_name}/#{stream_name}\" to #{local_dir.inspect}")

    each_event_in_stream(group_name, stream_name) do |event|
      message = event.message
      message_size = message.bytesize
      in_bytes += message_size

      # Apply JSON Path filter
      if !scrub_jsonpaths.nil?
        message = scrub_message_by_jsonpaths(message, jsonpaths)
      end

      # Apply gsub filter second to reduce chance of JSON breaking
      if !scrub_gsub.nil?
        message = scrub_message_by_gsub(message, scrub_gsub)
      end

      out_bytes += message.bytesize

      # Transform the event into a logstash/syslog like format
      output_data = serialize_event(event.timestamp, stream_name, message)

      # TODO - File writer should be its own generator
      # Open a new file if no file yet or if we would exceed the split size
      if cur_file.nil? || \
         (split_bytes && (cur_file_bytes + output_data.bytesize > split_bytes))

        if split_bytes
          f_base = stream_name + format('_part.%04d.log', next_file_index)
        else
          f_base = stream_name + '.log'
        end

        filename = File.join(local_dir, f_base)

        log.info("Creating new output file #{filename.inspect}")
        cur_file = File.open(filename, File::WRONLY | File::CREAT | File::EXCL)
        filenames << filename

        cur_file_bytes = 0
        next_file_index += 1
      end

      # Write the event to the file
      cur_file.write(output_data)
      cur_file_bytes += output_data.bytesize

      event_count += 1
      message_size != message.bytesize and scrubbed_count += 1
    end

    log.info("Wrote #{event_count} events (#{scrubbed_count} scrubbed) to #{filenames.length} files")

    {
      filenames: filenames,
      event_count: event_count,
      scrubbed_count: scrubbed_count,
      in_bytes: in_bytes,
      out_bytes: out_bytes,
    }
  end

  # @param [String] filename Full path to file to load events from
  def load_events_from_file(filename)
    # Load previously saved events from a file into a list of event
    # stream items containing a timestamp and message.
    stream_events = []
    File.readlines(filename).each do |line|
      timestamp, stream_name, message = deserialize_event(line)
      stream_events << {timestamp: timestamp, message: message}
    end
 
    stream_events
  end

  # @param [String] s3_url Full URL to bucket, including path
  #
  # @return [Array] s3_bucket, s3_directory
  def parse_s3_url(s3_url)
    # Remove prefix if present and split into bucket and path halfs
    (s3_bucket, s3_directory) = s3_url.gsub(/^s3:\/\//, '').split('/', 2)
    # Remove surrounding slashes from directory for easy joins
    unless s3_directory.nil?
      s3_directory = s3_directory.gsub(/^\/+/, '').gsub(/\/+$/, '')
    end

    [s3_bucket, s3_directory]
  end

  # @param [String] filename File to upload
  # @param [String] s3_url Full S3 URL to upload to
  def upload_file_to_s3(filename, s3_url)
    (s3_bucket, s3_directory) = parse_s3_url(s3_url)

    s3_path = [s3_directory, File.basename(filename)].join('/')

    log.info("Uploading #{filename.inspect} to \"s3://#{s3_bucket}/#{s3_path}\"#{dry_tag}")

    File.open(filename, 'rb') do |file|
      s3_client.put_object(
        bucket: s3_bucket,
        key: s3_path,
        body: file
      )
    end
  end

  # @param [String] group_name Name of log group to delete stream from
  # @param [String] stream_name Name of stream to delete
  def delete_cloudwatch_stream(group_name, stream_name)
    log.info("Deleting stream \"#{group_name}/#{stream_name}\"#{dry_tag}")
    
    cloudwatch_client_rw.delete_log_stream(
      log_group_name: group_name,
      log_stream_name: stream_name
    )
  end

  # @param [String] group_name Name of log group to create stream in
  # @param [String] stream_name Name of stream to create
  def create_new_cloudwatch_stream(group_name, stream_name)
    log.info("Creating stream \"#{group_name}/#{stream_name}\"#{dry_tag}")

    cloudwatch_client_rw.create_log_stream(
      log_group_name: group_name,
      log_stream_name: stream_name
    )
  end

  # @param [String] group_name Name of log group to create stream in
  # @param [String] stream_name Name of stream to create
  def upload_events_to_cloudwatch_stream(group_name, stream_name, events)
    # Upload a set of stream events with timestamp and message keys
    # to a given log stream.
    log.info("Adding log events for  \"#{group_name}/#{stream_name}\"#{dry_tag}")

    cloudwatch_client_rw.put_log_events(
        log_group_name: group_name,
        log_stream_name: stream_name,
        log_events: events
    )
  end

  # @param [String] group_name Name of log group to replace the stream in
  # @param [String] stream_name Name of stream to replace or create
  # @param [Array] filenames List of paths to saved log files to load into stream
  # @param [Bool] replace Set to true to recreate stream instead of just appending events
  #
  # @return [Array] List of files that were successfully processed
  def upload_eventfiles_to_cloudwatch_stream(group_name, stream_name, filenames, replace: false)
    processed = []

    filenames.each do |filename|
      events = load_events_from_file(filename)
      events.length || next

      if replace
        delete_cloudwatch_stream(group_name, stream_name)
      end

      create_new_cloudwatch_stream(group_name, stream_name)

      upload_events_to_cloudwatch_stream(group_name, stream_name, events)
 
      processed << filename
    end

    processed
  end

  def delete_local_file(filename)
    log.info("rm #{filename.inspect}")
    File.unlink(filename)
  end

  def humansize(size)
    # Credit to FilipeC https://stackoverflow.com/a/47486815
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'Pib', 'EiB']

    return '0.0 B' if size == 0

    exp = (Math.log(size) / Math.log(1024)).to_i
    exp += 1 if size.to_f / 1024**exp >= 1024 - 0.05
    exp = 6 if exp > 6

    '%.1f %s' % [size.to_f / 1024**exp, units[exp]]
  end
end
