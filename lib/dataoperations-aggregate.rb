require 'logger'
require 'time'
require 'descriptive_statistics'
module DataOperations
  class Aggregate
      DEFAULT_TIME_FORMAT='%Y-%m-%dT%H:%M:%S.%L%:z'
      DEFAULT_TIME_FIELD='timestamp'
      DEFAULT_OUTPUT_TIME_FORMAT='%Y-%m-%dT%H:%M:%S.%L%z'
      DEFAULT_INTERVALS=[10]
      DEFAULT_FLUSH_INTERVAL=5
      DEFAULT_PROCESSING_MODE=:batch
      DEFAULT_FIELD_NO_DATA_VALUE='no_data'
      DEFAULT_AGGREGATIONS=['sum','min','max','mean','median','variance','standard_deviation']
      VALID_AGGREGATIONS = ['sum','min','max','mean','median','variance','standard_deviation']
      DEFAULT_HASH_TIME_FORMAT = '%Y-%m-%dT%H'
      DEFAULT_INERVAL_SECONDS = 3600

      def initialize(aggregator: {},
                    time_format: DEFAULT_TIME_FORMAT,
                    time_field: DEFAULT_TIME_FIELD,
                    output_time_format: DEFAULT_OUTPUT_TIME_FORMAT,
                    intervals:DEFAULT_INTERVALS,
                    flush_interval:DEFAULT_FLUSH_INTERVAL,
                    keep_interval:DEFAULT_KEEP_INTERVAL,
                    field_no_data_value:DEFAULT_FIELD_NO_DATA_VALUE,
                    processing_mode:DEFAULT_PROCESSING_MODE,
                    aggregator_name:nil,
                    log:Logger.new(STDOUT),
                    aggregation_names:,
                    group_field_names:,
                    aggregate_field_names:
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
          @aggregator_name = aggregator_name
          
          
          if aggregation_names.nil? || ! aggregation_names.is_a?(Array)
              raise "Configuration error, aggregation_names must be specified and Array"
          end
          if group_field_names.nil? || ! aggregation_names.is_a?(Array)
              raise "Configuration error, group_field_names must be specified and Array"
          end
          if aggregate_field_names.nil? || ! aggregation_names.is_a?(Array)
              raise "Configuration error, aggregate_field_names must be specified and Array"
          end

          @log = log

          @hash_time_format = DEFAULT_HASH_TIME_FORMAT
          @interval_seconds = DEFAULT_INERVAL_SECONDS

          @aggregation_names = aggregation_names
          @group_field_names = group_field_names
          @aggregate_field_names = aggregate_field_names

          @aggregation_names.each {|operation|
              if ! VALID_AGGREGATIONS.include?(operation)
                raise "aggregations must set any combination of sum,min,max,mean,median,variance,standard_deviation"
              end
          }
          @intervals.each {|interval|
            if ! (interval % @intervals[0] == 0)
              raise "interval: #{interval} must be multiple of first interval: #{@intervals[0]}"
            end
          }

          #TODO:
          # - Duplicate intervals - Done
          # - Sort intervals - Done
          # - Validate aggregation_names, group_field_names, aggregate_field_names
      end

      def log_level(log_level)
          @log.level = log_level
      end

      def add_events(record)
          timestamp = nil
          if ! record.has_key?(@time_field) || ! (timestamp = DateTime.strptime(record[@time_field],@time_format).to_time.to_i)
            timestamp = DateTime.now.to_time.to_i
          end

          current_interval_seconds = (timestamp / @intervals[0]) * @intervals[0]
          aggregator_hash_key = current_interval_seconds

          hash_group_key = nil
          @group_field_names.each {|field_name|
            if ! hash_group_key.nil?
              hash_group_key = "#{hash_group_key}_#{field_name}:#{record[field_name]}"
            else
              hash_group_key = "#{field_name}:#{record[field_name]}"
            end
          }

          aggregator_item={}
          if @aggregator.has_key?(hash_group_key)
            aggregator_item = @aggregator[hash_group_key]
          else
            group_detail = {}
            aggregate_detail = {}
            interval_detail = {}
            @group_field_names.each {|field_name|
              if record.has_key?(field_name)
                group_detail[field_name] = record[field_name]
              else
                group_detail[field_name] = @field_no_data_value
              end
            }

            #Add interval empty data
            @intervals.each{|interval|
              interval_detail[interval.to_s]={}
            }

            aggregator_item["group_fields"]=group_detail
            aggregator_item["aggregate_fields"]=aggregate_detail
            aggregator_item["intervals"]=interval_detail

            @aggregator[hash_group_key]=aggregator_item
          end

          if ! aggregator_item["aggregate_fields"].has_key?(aggregator_hash_key)
            hash_aggregator = {}
            hash_aggregator[:time_started]=Time.now.to_i
            hash_aggregator["processed"]=1
            aggregator_item["aggregate_fields"][aggregator_hash_key]=hash_aggregator
          else
            aggregator_item["aggregate_fields"][aggregator_hash_key]["processed"]+=1
          end

          @aggregate_field_names.each {|field_name|
            aggregate_values = []
            if aggregator_item["aggregate_fields"][aggregator_hash_key].has_key?(field_name)
              aggregate_values = aggregator_item["aggregate_fields"][aggregator_hash_key][field_name]
            end
            if record[field_name].is_a?(Integer) or record[field_name].is_a?(Float)
              aggregate_values << record[field_name]
            else
              aggregate_values << 0
            end
            aggregator_item["aggregate_fields"][aggregator_hash_key][field_name] = aggregate_values
          } 
      end

      def aggregate_data
          @aggregator
      end

      def aggregate_events
          aggregate_data = {}

          #@log.debug @aggregator
          #@aggregator_mutex.synchronize do
          current_time = Time.now.to_i
          @aggregator.each {|group_item_key,group_item_value|
                group_item_value["aggregate_fields"].each {|aggregator_item_key,aggregator_item_value|
                      #If processing mode is :batch, aggregate immediatly, else wait to arrive events (streaming processing like fluentd)
                      @processing_mode == :batch ? limit_time = 0 : limit_time = aggregator_item_value[:time_started] + @intervals[0] + @keep_interval

                      #Is this data ready to aggregate (based on the ingest time), if @processing_mode is batch limit_time is 0
                      if current_time >= limit_time
                          aggregator_data = {}
                          aggregator_data[@time_field] = Time.at(aggregator_item_key).strftime(@output_time_format)
                          aggregator_data.merge!(group_item_value["group_fields"])

                          aggregator_data["time"] = aggregator_item_key
                          aggregator_data["processed"] = aggregator_item_value["processed"]
                          aggregator_data["aggregator_id"] = @aggregator_name if @aggregator_name
                          
                          #Add entry in accumulative aggregation hash
                          group_item_value['intervals'].keys[1..-1].each{|interval_secs|
                              interval_aggregator_item_key=(aggregator_item_key/interval_secs.to_i)*interval_secs.to_i
                              #@log.debug  "interval_aggregator_item_key: #{interval_aggregator_item_key}"

                              if interval_aggregator_item_value = group_item_value['intervals'][interval_secs][interval_aggregator_item_key]
                                  interval_aggregator_item_value[:time_started] = aggregator_item_value[:time_started] if interval_aggregator_item_value[:time_started] < aggregator_item_value[:time_started]
                                  interval_aggregator_item_value["processed"] += aggregator_item_value["processed"]
                                  #@log.debug interval_aggregator_item_value
                              else 
                                  interval_aggregator_item_value = {}
                                  interval_aggregator_item_value[:time_started] = aggregator_item_value[:time_started]
                                  interval_aggregator_item_value["aggregate_fields"]={}
                                  interval_aggregator_item_value["processed"] = aggregator_item_value["processed"]
                                  group_item_value['intervals'][interval_secs][interval_aggregator_item_key] = interval_aggregator_item_value
                                  #@log.debug interval_aggregator_item_value
                              end
                          }

                          aggregator_item_value.each { |aggregate_field_key,aggregate_field_value|                 
                              #Create field metadata for subsecuents aggregations
                              group_item_value['intervals'].keys[1..-1].each{|interval_secs|
                                  interval_aggregator_item_key=(aggregator_item_key/interval_secs.to_i)*interval_secs.to_i
                                  interval_aggregator_item_value = group_item_value['intervals'][interval_secs][interval_aggregator_item_key]
                                  #@log.debug interval_aggregator_item_value
                                  if ! interval_aggregator_item_value["aggregate_fields"].has_key?(aggregate_field_key) && aggregate_field_value.is_a?(Array)
                                      interval_aggregator_item_value["aggregate_fields"][aggregate_field_key]={}
                                      @aggregation_names.each {|operation|
                                        interval_aggregator_item_value["aggregate_fields"][aggregate_field_key][operation]=[]
                                      }
                                  end
                              }

                              #Aggregate data
                              if aggregate_field_value.is_a?(Array)
                                  @aggregation_names.each {|operation|
                                    data = aggregate_field_value.method(operation).call
                                    aggregator_data["#{aggregate_field_key}_#{operation}"] = data

                                    #Add aggregated data to interval
                                    group_item_value['intervals'].keys[1..-1].each{|interval_secs|
                                      interval_aggregator_item_key=(aggregator_item_key/interval_secs.to_i)*interval_secs.to_i
                                      interval_aggregator_item_value = group_item_value['intervals'][interval_secs][interval_aggregator_item_key]
                                      interval_aggregator_item_value["aggregate_fields"][aggregate_field_key][operation] << data
                                    }
                                  }
                              end
                          }

                          group_item_value["aggregate_fields"].delete(aggregator_item_key)
                          aggregate_data[group_item_value['intervals'].keys[0]] =[] if aggregate_data[group_item_value['intervals'].keys[0]].nil?
                          aggregate_data[group_item_value['intervals'].keys[0]] << aggregator_data
                      end
                }

              #Calculate subsecuents aggregations
              group_item_value["intervals"].keys[1..-1].each {|s_interval|
                  group_item_value["intervals"][s_interval].each{|aggregator_item_key,aggregator_item_value|
                      interval = s_interval.to_i
                      #If processing mode is :batch, aggregate immediatly, else wait to arrive events (streaming processing like fluentd)
                      @processing_mode == :batch ? limit_time = 0 : limit_time = aggregator_item_value[:time_started] + interval + @keep_interval

                      #@log.debug "processing_mode:#{@processing_mode} limit_time:#{limit_time}"

                      if current_time >= limit_time
                          aggregator_data = {}
                          aggregator_data[@time_field] = Time.at(aggregator_item_key).strftime(@output_time_format)
                          aggregator_data.merge!(group_item_value["group_fields"])

                          aggregator_data["time"] = aggregator_item_key
                          aggregator_data["processed"] = aggregator_item_value["processed"]
                          aggregator_data["aggregator_id"] = @aggregator_name if @aggregator_name
                          aggregator_item_value["aggregate_fields"].each{|field_name,field_data|
                              field_data.each{|operation,vector|
                                  case operation
                                  when 'max','min','mean','median'
                                      data = vector.method(operation).call
                                  else
                                      data = vector.median
                                  end
                                  aggregator_data["#{field_name}_#{operation}"] = data
                              }
                          }
                          #@log.debug aggregator_item_value
                          #@log.debug aggregator_data
                          group_item_value["intervals"][s_interval].delete(aggregator_item_key)
                          aggregate_data[s_interval] =[] if aggregate_data[s_interval].nil?
                          aggregate_data[s_interval] << aggregator_data
                      end
                  } 
              }
          }

          #@log.debug aggregate_data
          unless aggregate_data.empty?
            aggregate_data
          end
      #rescue Exception => e
      #  $log.error e
      end
  end
end
