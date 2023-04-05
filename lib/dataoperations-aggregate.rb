require 'logger'
require 'time'
require 'descriptive_statistics'
module DataOperations
  class Aggregate
    DEFAULT_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%L%:z'.freeze
    DEFAULT_TIME_FIELD = 'timestamp'.freeze
    DEFAULT_OUTPUT_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%L%z'.freeze
    DEFAULT_INTERVALS = [10].freeze
    DEFAULT_FLUSH_INTERVAL = 5
    DEFAULT_PROCESSING_MODE = :batch
    DEFAULT_TIME_STARTED_MODE = :first_message
    DEFAULT_FIELD_NO_DATA_VALUE = 'no_data'.freeze
    DEFAULT_AGGREGATIONS = %w[sum min max mean median variance standard_deviation].freeze
    VALID_AGGREGATIONS = %w[sum min max mean median variance standard_deviation bucket].freeze
    DEFAULT_HASH_TIME_FORMAT = '%Y-%m-%dT%H'.freeze
    DEFAULT_INERVAL_SECONDS = 3600

    def initialize(aggregator: {},
                   time_format: DEFAULT_TIME_FORMAT,
                   time_field: DEFAULT_TIME_FIELD,
                   output_time_format: DEFAULT_OUTPUT_TIME_FORMAT,
                   intervals: DEFAULT_INTERVALS,
                   flush_interval: DEFAULT_FLUSH_INTERVAL,
                   keep_interval: DEFAULT_KEEP_INTERVAL,
                   field_no_data_value: DEFAULT_FIELD_NO_DATA_VALUE,
                   processing_mode: DEFAULT_PROCESSING_MODE,
                   time_started_mode: DEFAULT_TIME_STARTED_MODE,
                   aggregator_name: nil,
                   log: Logger.new(STDOUT),
                   aggregation_names:,
                   group_field_names:,
                   aggregate_field_names:,
                   buckets:[],
                   bucket_metrics:[]
                )
      @aggregator = aggregator
      @time_format = time_format
      @time_field = time_field
      @output_time_format = output_time_format
      @intervals = intervals.uniq.sort!
      @flush_interval = flush_interval
      @keep_interval = keep_interval
      @field_no_data_value = field_no_data_value
      @processing_mode = processing_mode
      @time_started_mode = time_started_mode
      @aggregator_name = aggregator_name
      @buckets = buckets
      @bucket_metrics = bucket_metrics


      if aggregation_names.nil? || !aggregation_names.is_a?(Array)
        raise 'Configuration error, aggregation_names must be specified and Array'
      end
      if group_field_names.nil? || !aggregation_names.is_a?(Array)
        raise 'Configuration error, group_field_names must be specified and Array'
      end
      if aggregate_field_names.nil? || !aggregation_names.is_a?(Array)
        raise 'Configuration error, aggregate_field_names must be specified and Array'
      end

      @log = log

      @hash_time_format = DEFAULT_HASH_TIME_FORMAT
      @interval_seconds = DEFAULT_INERVAL_SECONDS

      @aggregation_names = aggregation_names
      @group_field_names = group_field_names
      @aggregate_field_names = aggregate_field_names

      @aggregation_names.each do |operation|
        unless VALID_AGGREGATIONS.include?(operation)
          raise 'aggregations must set any combination of sum,min,max,mean,median,variance,standard_deviation'
        end
      end
      @intervals.each do |interval|
        unless (interval % @intervals[0]).zero?
          raise "interval: #{interval} must be multiple of first interval: #{@intervals[0]}"
        end
      end

      @aggregator_mutex = Mutex.new
      # TODO:
      # - Duplicate intervals - Done
      # - Sort intervals - Done
      # - Validate aggregation_names, group_field_names, aggregate_field_names
    end

    def log_level(log_level)
      @log.level = log_level
    end

    def add_events(record)
      timestamp = nil
      if !record.key?(@time_field) || !(timestamp = DateTime.strptime(record[@time_field], @time_format).to_time.to_i)
        timestamp = DateTime.now.to_time.to_i
      end

      current_interval_seconds = (timestamp / @intervals[0]) * @intervals[0]
      aggregator_hash_key = current_interval_seconds

      hash_group_key = nil
      @group_field_names.each do |field_name|
        hash_group_key = !hash_group_key.nil? ? "#{hash_group_key}_#{field_name}:#{record[field_name]}" : "#{field_name}:#{record[field_name]}"
      end

      aggregator_item = {}
      if @aggregator.key?(hash_group_key)
        aggregator_item = @aggregator[hash_group_key]
      else
        group_detail = {}
        aggregate_detail = {}
        interval_detail = {}
        @group_field_names.each do |field_name|
          group_detail[field_name] = record.key?(field_name) ? record[field_name] : @field_no_data_value
        end

        # Add interval empty data
        @intervals.each do |interval|
          interval_detail[interval.to_s] = {}
        end

        aggregator_item['group_fields'] = group_detail
        aggregator_item['aggregate_fields'] = aggregate_detail
        aggregator_item['intervals'] = interval_detail

	@aggregator_mutex.synchronize {@aggregator[hash_group_key] = aggregator_item}
      end

      if !aggregator_item['aggregate_fields'].key?(aggregator_hash_key)
        hash_aggregator = {}
        hash_aggregator[:time_started] = Time.now.to_i
        hash_aggregator['processed'] = 1
	@aggregator_mutex.synchronize {aggregator_item['aggregate_fields'][aggregator_hash_key] = hash_aggregator}
      else
        aggregator_item['aggregate_fields'][aggregator_hash_key]['processed'] += 1
      end

      @aggregate_field_names.each do |field_name|
        aggregate_values = []
        if aggregator_item['aggregate_fields'][aggregator_hash_key].key?(field_name)
          aggregate_values = aggregator_item['aggregate_fields'][aggregator_hash_key][field_name]
        end
        if record[field_name].is_a?(Integer) || record[field_name].is_a?(Float)
          aggregate_values << record[field_name]
        else
          aggregate_values << 0
        end
        aggregator_item['aggregate_fields'][aggregator_hash_key][field_name] = aggregate_values
      end
    end

    def aggregate_data
      @aggregator
    end

    def aggregate_events
      aggregate_data = {}

      # @log.debug @aggregator
      @aggregator_mutex.synchronize do
        current_time = Time.now.to_i
        @aggregator.each do |group_item_key, group_item_value|
          aggregate_first_interval(aggregate_data, current_time, group_item_value)

          # Calculate subsecuents aggregations
          group_item_value['intervals'].keys[1..-1].each do |s_interval|
            aggregate_subsequents_intervals(aggregate_data, current_time, group_item_value, s_interval)
          end
        end
      end

      # @log.debug aggregate_data
      aggregate_data unless aggregate_data.empty?
      # rescue Exception => e
      #  $log.error e
    end

    private

    def aggregate_first_interval(aggregate_data, current_time, group_item_value)
      group_item_value['aggregate_fields'].each do |aggregator_item_key, aggregator_item_value|
        # If processing mode is :batch, aggregate immediatly, else wait to arrive events (streaming processing like fluentd)
        @processing_mode == :batch ? limit_time = 0 : limit_time = aggregator_item_value[:time_started] + @intervals[0] + @keep_interval

        # Is this data ready to aggregate (based on the ingest time), if @processing_mode is batch limit_time is 0
        next unless current_time >= limit_time

        aggregator_data = {}
        aggregator_data[@time_field] = Time.at(aggregator_item_key).strftime(@output_time_format)
        aggregator_data.merge!(group_item_value['group_fields'])

        aggregator_data['time'] = aggregator_item_key
        aggregator_data['processed'] = aggregator_item_value['processed']
        if @aggregator_name
          aggregator_data['aggregator_id'] = @aggregator_name
        end

        # Add entry in accumulative aggregation hash
        group_item_value['intervals'].keys[1..-1].each do |interval_secs|
          create_aggregation_hash(aggregator_item_key, aggregator_item_value, group_item_value, interval_secs)
        end

        aggregator_item_value.each do |aggregate_field_key, aggregate_field_value|
          execute_aggregation(aggregate_field_key, aggregate_field_value, aggregator_data, aggregator_item_key, group_item_value)
        end

        group_item_value['aggregate_fields'].delete(aggregator_item_key)
        if aggregate_data[group_item_value['intervals'].keys[0]].nil?
          aggregate_data[group_item_value['intervals'].keys[0]] = []
        end
        aggregate_data[group_item_value['intervals'].keys[0]] << aggregator_data
      end
    end

    def execute_aggregation(aggregate_field_key, aggregate_field_value, aggregator_data, aggregator_item_key, group_item_value)
      # Create field metadata for subsecuents aggregations
      create_metadata_aggregation(aggregate_field_key,
                                  aggregate_field_value,
                                  aggregator_data,
                                  aggregator_item_key,
                                  group_item_value)
      # Aggregate data
      if aggregate_field_value.is_a?(Array)
        @aggregation_names.each do |operation|
          
          #If bucket, calculate bucket for metric
          if operation == 'bucket'
            #If set buckets and set metrics to calculate (bucket values depends of ranges based in metric activity)
            if !@buckets.nil? && !@bucket_metrics.nil? && @bucket_metrics.include?(aggregate_field_key)
              data_bucket = calculate_buckets(aggregate_field_value, @buckets)
             
              data_bucket.each {|bucket,bucket_count|
                #@log.info("#{aggregate_field_key}_#{bucket} = #{bucket_count}")
                aggregator_data["#{aggregate_field_key}_bucket#{bucket}"] = bucket_count
              }

              # Add aggregated data to interval
              group_item_value['intervals'].keys[1..-1].each do |interval_secs|

                interval_aggregator_item_key = (aggregator_item_key / interval_secs.to_i) * interval_secs.to_i
                interval_aggregator_item_value = group_item_value['intervals'][interval_secs][interval_aggregator_item_key]
                data_bucket.each {|bucket,bucket_count|
                  #@log.info("#{aggregate_field_key}_#{bucket} = #{bucket_count}")
                  interval_aggregator_item_value['aggregate_fields'][aggregate_field_key]["bucket#{bucket}"] = [] if interval_aggregator_item_value['aggregate_fields'][aggregate_field_key]["bucket#{bucket}"].nil?
                  interval_aggregator_item_value['aggregate_fields'][aggregate_field_key]["bucket#{bucket}"] << bucket_count
                }
                  
              end

            end
          else
            data = aggregate_field_value.method(operation).call
            aggregator_data["#{aggregate_field_key}_#{operation}"] = data

            # Add aggregated data to interval
            group_item_value['intervals'].keys[1..-1].each do |interval_secs|
              interval_aggregator_item_key = (aggregator_item_key / interval_secs.to_i) * interval_secs.to_i
              interval_aggregator_item_value = group_item_value['intervals'][interval_secs][interval_aggregator_item_key]
              interval_aggregator_item_value['aggregate_fields'][aggregate_field_key][operation] << data
            end
          end


        end

        if !@buckets.nil? && ! @bucket_metrics.nil?
          #data = calculate_buckets(data, @buckets)
        end

      end
    end

    def create_metadata_aggregation(aggregate_field_key, aggregate_field_value, aggregator_data, aggregator_item_key, group_item_value)
      group_item_value['intervals'].keys[1..-1].each do |interval_secs|
        interval_aggregator_item_key = (aggregator_item_key / interval_secs.to_i) * interval_secs.to_i
        interval_aggregator_item_value = group_item_value['intervals'][interval_secs][interval_aggregator_item_key]

        # @log.debug interval_aggregator_item_value
        next unless !interval_aggregator_item_value['aggregate_fields'].key?(aggregate_field_key) && aggregate_field_value.is_a?(Array)

        interval_aggregator_item_value['aggregate_fields'][aggregate_field_key] = {}
        @aggregation_names.each do |operation|
          interval_aggregator_item_value['aggregate_fields'][aggregate_field_key][operation] = []
        end

        ##Add buckets metadata (empty hash)
        ##interval_aggregator_item_value['aggregate_fields'][aggregate_field_key]['buckets']={}
      end
    end

    def create_aggregation_hash(aggregator_item_key, aggregator_item_value, group_item_value, interval_secs)
      interval_aggregator_item_key = (aggregator_item_key / interval_secs.to_i) * interval_secs.to_i
      # @log.debug  "interval_aggregator_item_key: #{interval_aggregator_item_key}"

      if interval_aggregator_item_value = group_item_value['intervals'][interval_secs][interval_aggregator_item_key]
        if @time_started_mode == :first_event && aggregator_item_value[:time_started] < interval_aggregator_item_value[:time_started]
          interval_aggregator_item_value[:time_started] = aggregator_item_value[:time_started]
        elseif @time_started_mode == :last_event && aggregator_item_value[:time_started] > interval_aggregator_item_value[:time_started]
          interval_aggregator_item_value[:time_started] = aggregator_item_value[:time_started]
        end
        interval_aggregator_item_value['processed'] += aggregator_item_value['processed']
        # @log.debug interval_aggregator_item_value
      else
        interval_aggregator_item_value = {}
        interval_aggregator_item_value[:time_started] = aggregator_item_value[:time_started]
        interval_aggregator_item_value['aggregate_fields'] = {}
        interval_aggregator_item_value['processed'] = aggregator_item_value['processed']
        group_item_value['intervals'][interval_secs][interval_aggregator_item_key] = interval_aggregator_item_value
        # @log.debug interval_aggregator_item_value
      end
    end

    def aggregate_subsequents_intervals(aggregate_data, current_time, group_item_value, s_interval)
      group_item_value['intervals'][s_interval].each do |aggregator_item_key, aggregator_item_value|
        acumulative_aggregation(aggregate_data, aggregator_item_key, aggregator_item_value, current_time, group_item_value, s_interval)
      end
    end

    def acumulative_aggregation(aggregate_data, aggregator_item_key, aggregator_item_value, current_time, group_item_value, s_interval)
      interval = s_interval.to_i
      # If processing mode is :batch, aggregate immediatly, else wait to arrive events (streaming processing like fluentd)
      limit_time = @processing_mode == :batch ? 0 : aggregator_item_value[:time_started] + interval + @keep_interval

      # @log.debug "processing_mode:#{@processing_mode} limit_time:#{limit_time}"

      unless current_time < limit_time
        aggregator_data = {}
        aggregator_data[@time_field] = Time.at(aggregator_item_key).strftime(@output_time_format)
        aggregator_data.merge!(group_item_value['group_fields'])

        aggregator_data['time'] = aggregator_item_key
        aggregator_data['processed'] = aggregator_item_value['processed']
        if @aggregator_name
          aggregator_data['aggregator_id'] = @aggregator_name
        end
        aggregator_item_value['aggregate_fields'].each do |field_name, field_data|
          field_data.each do |operation, vector|
            case operation
            when 'max', 'min', 'mean', 'median'
              data = vector.method(operation).call
            when 'bucket'
              #Bucket operation generate bucket[\d]+
              data = nil
            when /^bucket[\d]+/
              #For buckets sum accumulations for internvals
              data = vector.method('sum').call
            else
              data = vector.median
            end
            #Nil data is avoid (for example for 'bucket' name operation)
            aggregator_data["#{field_name}_#{operation}"] = data unless data.nil?
          end
        end
        # @log.debug aggregator_item_value
        # @log.debug aggregator_data
        group_item_value['intervals'][s_interval].delete(aggregator_item_key)
        aggregate_data[s_interval] = [] if aggregate_data[s_interval].nil?
        aggregate_data[s_interval] << aggregator_data
      end
    end
    
    #Return Array with count by each bucket
    def calculate_buckets(data, buckets_config)
      buckets_config.sort!.uniq!
      buckets = {}
    
      buckets_config.each {|bucket| buckets[bucket] = 0}
    
      data.each {|item|
        buckets_config.each {|bucket|
          if item <= bucket
            buckets[bucket] += 1
            next
          end
        }
      }
      return buckets
    end

  end
end
