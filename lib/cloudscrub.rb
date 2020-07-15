require 'date'
require 'logger'
require 'json'
require 'jsonpath'

#require 'aws-sdk'
require 'aws-sdk-cloudwatchlogs'
require 'aws-sdk-s3'

require 'cloudscrub/version'

# Set of utilities for download, scrub, S3 stashing, and replacement of
# CloudWatch log streams.
class CloudScrub
  
  # @param [Bool] cli_mode Set true for cli style logging
  # @param [Filehandle] log_to Filehandle to write to
  # @param [Integer] log_level Logger log level
  # @param [String] aws_log_file Debug log file for AWS calls
  # @param [Bool] dry_run Read but don't write to any AWS resource
  def initialize(cli_mode: false, log_to: STDERR, log_level: Logger::INFO, aws_log_file: nil, dry_run: false)
    @cli_mode = cli_mode
    @log_to = log_to
    @log_level = log_level
    @dry_run = dry_run

    # AWS logging is uncontrollably verbose so it goes to a file
    if aws_log_file
      enable_aws_logging(aws_log_file, log_level: log_level)
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
      @log.progname = (defined? self.name) ? self.name : self.class.inspect
    end

    @log
  end

  # @param [String] aws_log_file File to capture AWS call logs in
  # @param [Integer] log_level Logger level - Only DEBUG (0) changes behavior
  def enable_aws_logging(aws_log_file, log_level: Logger::INFO)
    if log_level != Logger::DEBUG
      # For non-debug, create a modified short logger that does not add an extra \n
      aws_log_formatter = Aws::Log::Formatter.new(
        "[:client_class :http_response_status_code :time] :operation :error_class",
        max_string_size: 1000
      )
    else
      # The default format is VERY verbose which is OK for debugging
      aws_log_formatter = Aws::Log::Formatter.default
    end

    # Setup default AWS logger - Defaults to INFO and seems to ignore other levels :(
    Aws.config.update({:logger => Logger.new(aws_log_file),
                       :log_formatter => aws_log_formatter})
  end

  # @return [Aws::CloudWatchLogs::Client] On-demand initialized CloudWatch
  #                                       client for reading (ignores dry_run)
  def cloudwatch_client_ro
    @cloudwatch_client_ro ||= Aws::CloudWatchLogs::Client.new(retry_mode: 'adaptive')
  end

  # @return [Aws::CloudWatchLogs::Client] On-demand initialized CloudWatch
  #                                       client for writing (honors dry_run)
  def cloudwatch_client_rw
    @cloudwatch_client_rw ||= Aws::CloudWatchLogs::Client.new(
      retry_mode: 'adaptive',
      stub_responses: @dry_run
    )
  end

  # @return [Aws::S3::Client] On-demand initialized S3 client (honors dry_run)
  def s3_client
    @s3_client ||= Aws::S3::Client.new(retry_mode: 'adaptive', stub_responses: @dry_run)
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
  # @return [String, Bool] Message with matching text removed.  Last element is true
  #                        if the message was altered.
  def scrub_message_by_gsub(message, scrub_gsub)
    # Apply regular expressions to a line allowing arbitrary changes to the
    nmessage = message.gsub(scrub_gsub, '')
    [nmessage, nmessage != message]
  end

  # @param [String] message Event message
  # @param [Hash] jsonpaths Hash with keys of string JSON path
  #                         and values of matching JsonPath objects
  # @param [Bool] return_raw Return message as hash (decoded) instead
  #                          of a re-encoded string
  # @return [Array[String|Hash, Bool] Message with matching JSON elements removed
  #                                   If return_raw is set it will be a JSON serializable
  #                                   hash instead of encoded JSON if the message parses,
  #                                   Second element is false if unchanged or true if filter
  #                                   matched.
  def scrub_message_by_jsonpaths(message, jsonpaths, return_raw: false)
    # Remove elements matching redact_paths from data, unless they match an
    # element in exclude_paths.  Use the jsonpath CLI tool to tune your
    # filters.
    if jsonpaths.nil?
      jsonpaths = []
    end

    changed = false

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
        changed = true
        json_message = json_message.delete(path_string)
      end
    end

    if return_raw
      return [json_message.to_hash, changed]
    end
  
    if changed
      message = json_message.to_hash.to_json(ascii_only: true)
    end

    [message, changed]
  end

  # @param [Integer] ms_timestamp Milliseconds since epoch
  # @param [Bool] date_only Set to true to return YYYY-MM-DD only
  #
  # @return [String] ISO8601 timestamp with milliseconds OR just date
  def stamp_ms_to_iso8601(ms_timestamp, date_only: false)
    t = Time.at(ms_timestamp * 0.001).utc
    date_only ? t.strftime('%F') : t.strftime('%FT%T.%LZ')
  end

  # @param [String] isotime ISO8601 string in the form YYYY-MM-DDTHH:MM:SS.mmZ
  #
  # @return [Integer] Milliseconds since epoch
  def iso8601_to_stamp_ms(isotime)
    (Time.parse(isotime).to_f * 1000).round
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

  # @param [Integer] timestamp ms since epoch
  # @param [String] group_name Log group name
  # @param [String] stream_name Original stream name
  # @param [String] message Event message
  #
  # @return [String] LF terminated JSON line ready to write to file
  def serialize_event(timestamp, group_name, stream_name, message)
    JSON.dump({
      '@timestamp' => stamp_ms_to_iso8601(timestamp),
      'type' => "cw_#{group_name}",
      'host' => { 'name' => stream_name},
      'events' => message
     }) + "\n"
  end

  # @param [String] Line from saved log file
  #
  # @return [Array[timestamp, group_name, stream_name, message]]
  def deserialize_event(logline)
    jdata = JSON.parse(logline.chomp)
  
    timestamp = jdata['@timestamp']
    group_name = jdata['type'].gsub(/^cw_/, '')
    stream_name = jdata['host']['name']
    message = jdata['events']

    [timestamp, group_name, stream_name, message]
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
  # @param [Bool] date_subfolders Set to true to place each stream directly under a datestamped
  #                               subfolder based on first log line
  # @param [String or Regex] scrub_gsub gsub expression to filter from logs
  # @param [Array] scrub_jsonpaths List of JSONPath expressions to delete from logs
  # @param [Bool] nest_json Set to true to nest message JSON instead of escaping
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
    date_subfolders: true,
    scrub_gsub: nil,
    scrub_jsonpaths: nil,
    nest_json: true
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

    log.info("Downloading log stream \"#{group_name}/#{stream_name}\"")

    each_event_in_stream(group_name, stream_name) do |event|
      message = event.message
      message_size = message.bytesize
      in_bytes += message_size
      changes = false

      # Apply gsub filter
      if !scrub_gsub.nil?
        message, changed = scrub_message_by_gsub(message, scrub_gsub)
        changes |= changed
      end
      
      # Decode JSON and apply JSON Path filter
      if nest_json || !scrub_jsonpaths.nil?
        message, changed = scrub_message_by_jsonpaths(message, jsonpaths, return_raw: nest_json)
        changes |= changed
      end

      # Transform the event into a logstash/syslog like format
      output_data = serialize_event(event.timestamp, group_name, stream_name, message)

      out_bytes += output_data.bytesize

      # TODO - File writer should be its own generator
      # Open a new file if no file yet or if we would exceed the split size
      if cur_file.nil? || \
         (split_bytes && (cur_file_bytes + output_data.bytesize > split_bytes))

        if split_bytes
          f_base = stream_name + format('_part.%04d.log', next_file_index)
        else
          f_base = stream_name + '.log'
        end

        if date_subfolders
          cur_local_dir = File.join(local_dir, stamp_ms_to_iso8601(event.timestamp, date_only: true))
          if !Dir.exist?(cur_local_dir)
            log.debug("Creating local log directory #{cur_local_dir}.inspect")
            Dir.mkdir(cur_local_dir)
          end
        else
          cur_local_dir = local_dir
        end

        filename = File.join(cur_local_dir, f_base)

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
      changes && scrubbed_count += 1
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
  # @param [Bool] keep_subfolder Retain the parent folder for each filename
  #                            
  # @return [Response] AWS client response
  def upload_file_to_s3(filename, s3_url, keep_subfolder: false)
    (s3_bucket, s3_directory) = parse_s3_url(s3_url)

    basepath, basename = File.split(filename)
    pathset = [basename]

    if keep_subfolder
      pathset.unshift(File.basename(basepath))
    end

    pathset.unshift(s3_directory)

    s3_path = pathset.join('/')

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
  # @return [Response] AWS client response
  def delete_cloudwatch_stream(group_name, stream_name)
    log.info("Deleting stream \"#{group_name}/#{stream_name}\"#{dry_tag}")
    
    cloudwatch_client_rw.delete_log_stream(
      log_group_name: group_name,
      log_stream_name: stream_name
    )
  end

  # TODO - Stream replacement is limited to the point of being nearly pointless.
  # Limitations here: https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
  # - Maximum events/PUT - 10000
  # - Maximum payload size - 1,048,576 bytes (1MB) - Calculated as sum(UTF-8 encoded event sizes) + n * 26 
  # - All events must be in order (sort on timestamp key)
  # - All events must be within a 24 hour window
  # - Events can be backdated up to 14 days OR the retention period, whichever is shorter
  # - Events are rate limited to 5/sec (~5MB of logs/sec - About 17GB/hour)
  #
  # Leaving vestigial code in case someone wants to address these items.
  #
  # @param [String] group_name Name of log group to create stream in
  # @param [String] stream_name Name of stream to create
  # @return [Response] AWS client response
  # def create_new_cloudwatch_stream(group_name, stream_name)
  #   log.info("Creating stream \"#{group_name}/#{stream_name}\"#{dry_tag}")
  #   cloudwatch_client_rw.create_log_stream(
  #     log_group_name: group_name,
  #     log_stream_name: stream_name
  #   )
  # end
  #
  # # @param [String] group_name Name of log group to create stream in
  # # @param [String] stream_name Name of stream to create
  # # @return [Response] AWS client response
  # def upload_events_to_cloudwatch_stream(group_name, stream_name, events)
  #   # Upload a set of stream events with timestamp and message keys
  #   # to a given log stream.
  #   log.info("Adding log events for  \"#{group_name}/#{stream_name}\"#{dry_tag}")
  #
  #   cloudwatch_client_rw.put_log_events({
  #       log_group_name: group_name,
  #       log_stream_name: stream_name,
  #       log_events: events
  #   })
  # end
  #
  # # @param [String] group_name Name of log group to replace the stream in
  # # @param [String] stream_name Name of stream to replace or create
  # # @param [Array] filenames List of paths to saved log files to load into stream
  # # @param [Bool] replace Set to true to recreate stream instead of just appending events
  # #
  # # @return [Array] List of files that were successfully processed
  # def upload_eventfiles_to_cloudwatch_stream(group_name, stream_name, filenames, replace: false)
  #   processed = []
  #
  #   filenames.each do |filename|
  #     events = load_events_from_file(filename)
  #     events.length || next
  #
  #     if replace
  #       delete_cloudwatch_stream(group_name, stream_name)
  #     end
  #
  #     create_new_cloudwatch_stream(group_name, stream_name)
  #
  #     upload_events_to_cloudwatch_stream(group_name, stream_name, events)
  #
  #     processed << filename
  #   end
  #
  #   processed
  # end

  # @param [String] filename Full path to file to delete
  def delete_local_file(filename)
    log.info("rm #{filename.inspect}")
    File.unlink(filename)
  end

end
