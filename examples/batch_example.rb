require '../lib/dataoperations-aggregate.rb'
require 'socket'


@time_format = '%Y-%m-%dT%H:%M:%S.%L%:z'
@time_field = 'timestamp'
@output_time_format = '%Y-%m-%dT%H:%M:%S.%L%z'
@flush_interval = 1
@keep_interval = 1
@field_no_data_value = 'no_data'
@processing_mode = 'online'
@time_started_mode = 'first_event'
@aggregator_suffix_name = 'woker_0'
@group_fields = ['app','server']
@aggregations = ['sum','min','max','bucket']
@aggregate_fields = ['response_time_ms']
@buckets = [50,100]
@bucket_metrics = ['response_time_ms']

VALID_AGGREGATIONS = ['sum','min','max','mean','median','variance','standard_deviation','bucket']

@intervals = [5,10]

@group_field_names = @group_fields
@aggregate_field_names = @aggregate_fields
@aggregation_names = @aggregations
@aggregator_name = "#{Socket.gethostname}"
@aggregator_name = "#{@aggregator_name}-#{@aggregator_suffix_name}" unless @aggregator_suffix_name.nil?

@aggregator = {}
@processing_mode_type=@processing_mode=='online' ? :batch : :online
@time_started_mode_type=@time_started_mode=='first_event' ? :fist_event : :last_event


@data_operations = DataOperations::Aggregate.new(aggregator: @aggregator,
        time_format: @time_format,
        time_field: @time_field,
        output_time_format: @output_time_format,
        intervals: @intervals,
        flush_interval: @flush_interval,
        keep_interval: @keep_interval,
        field_no_data_value: @field_no_data_value,
        processing_mode: @processing_mode_type,
        time_started_mode: @time_started_mode_type,
        log: Logger.new(STDOUT),
        aggregator_name: @aggregator_name,
        aggregation_names: @aggregation_names,
        group_field_names: @group_field_names,
        aggregate_field_names: @aggregate_field_names,
        buckets: @buckets,
        bucket_metrics: @bucket_metrics)

######## Load sample data
record = {'app'=>'app01','server'=>'server01','response_time_ms'=>100}
@data_operations.add_events(record)
record = {'app'=>'app01','server'=>'server01','response_time_ms'=>110}
@data_operations.add_events(record)
#sleep 1
record = {'app'=>'app01','server'=>'server01','response_time_ms'=>100}
@data_operations.add_events(record)
record = {'app'=>'app01','server'=>'server01','response_time_ms'=>110}
@data_operations.add_events(record)
#sleep 5

log = Logger.new(STDOUT)

log.info(@data_operations.aggregate_data)

log.info(@data_operations.aggregate_events)
