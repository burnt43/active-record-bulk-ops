module ActiveRecord
  module BulkOps
    class << self
      def comparable_ruby_version
        @comparable_ruby_version ||= RUBY_VERSION.split('.').map{|x| sprintf("%02d", x)}.join
      end
    end

    module Insertion
      # NOTE: The constructor takes a collection of records that have not yet
      #   been saved to the database. We make a few assumptions for the sake
      #   of speed:
      #   1. We assume that everything in the collection is an unsaved instance of
      #      ActiveRecord::Base.
      #   2. We assume that the table we are INSERTing into has 'created_at' and
      #      'updated_at' timstamp columns.
      #   3. We assume that all records in the collection have the same columns
      #      dirty. We only look at the first record and what columns are dirty
      #      and we assume every other record has the same columns dirty.
      #   If need be we can add options to the constructor to allow handling these
      #   assumptions.
      class Insertor
        # The STRING_SIZE_THRESHOLD should be a power of 2 less than the
        # ALLOCATED_STRING_SIZE.
        ALLOCATED_STRING_SIZE = 2**24
        STRING_SIZE_THRESHOLD = 2**23

        def initialize(
          collection=[],
          override_attributes: {},
          touch_created_at: false,
          touch_updated_at: false
        )
          @collection = collection
          return if @collection.empty?

          # add the timestamp columns to the override attributes
          complete_override_attributes =
            if touch_created_at || touch_updated_at
              timestamp = @collection[0].send(:current_time_from_proper_timezone)

              {created_at: timestamp, updated_at: timestamp}.merge(override_attributes)
            else
              override_attributes
            end

          # find the class for this collection. the collection needs to be
          # homogenous for the insert to work.
          @insertion_class = @collection[0]&.class

          # find the minimal set of columns we need to set
          column_names = @collection[0].send(:keys_for_partial_write).to_set

          # find the active record representation of the database columns
          # we need to set
          @columns = @insertion_class.columns.select { |c| column_names.member?(c.name) }

          # convert the override_attributes into usable mysql values
          converted_override_attributes = Hash[
            complete_override_attributes.stringify_keys.map do |key, value|
              converted_value = @insertion_class.defined_enums.dig(key, value.to_s) || value
              [key, @insertion_class.connection._quote(converted_value)]
            end
          ]

          # map active record columns to the values we will set for all records.
          @override_column_values = Hash[
            @insertion_class
            .columns
            .select { |c| converted_override_attributes.key?(c.name) }
            .map { |c| [c, converted_override_attributes[c.name]] }
          ]
        end

        def insert!
          # send each 'INSERT INTO' to the mysql server
          insert_line_hash.values.each do |mysql_string|
            @insertion_class.connection.execute(mysql_string)
          end
        end

        private

        def insert_line_hash
          return @insert_line_hash if @insert_line_hash

          return {} if @collection.empty?

          # keep track of different indices
          column_index = 0
          columns_max_index = (@columns.size + @override_column_values.size) - 1
          insert_line_hash_index = 0
          item_max_index = @collection.size - 1

          # create the prefix 'INSERT INTO...' for each SQL INSERT line
          insert_line_prefix = String.new
          insert_line_prefix << "INSERT INTO `#{@insertion_class.table_name}` ("

          (@columns + @override_column_values.keys).each do |column|
            insert_line_prefix << "`#{column.name}`"

            unless column_index == columns_max_index
              insert_line_prefix << ','
            end

            column_index += 1
          end

          insert_line_prefix << ') VALUES '

          # create a hash that maps indices to mysql strings. we don't want
          # this to be an array, because arrays will be contiguous in memory
          # and this will reduce memory operators and should be faster as a
          # hash
          insert_line_hash = {
            0 => String.new(ActiveRecord::BulkOps.comparable_ruby_version > "020600" ? {capacity: ALLOCATED_STRING_SIZE} : {})
          }
          current_insert_line = insert_line_hash[insert_line_hash_index]
          current_insert_line << insert_line_prefix

          # add mysql records
          @collection.each_with_index do |item, item_index|
            current_insert_line << '('

            column_index = 0

            # normal columns we must convert to a string that mysql understands
            @columns.each do |column|
              # If this column represents an enum, then we have to convert
              # to the integer value for the string, otherwise mysql will
              # interpret this as a 0 for the value no matter what.
              column_value_with_enum_conversion =
                if @insertion_class.defined_enums.key?(column.name)
                  @insertion_class.defined_enums.dig(column.name, item.send(column.name))
                else
                  item.send(column.name)
                end

              # Convert the value of the column to something mysql understands.
              column_value_for_db =
                @insertion_class
                .connection
                ._quote(column.cast_type.type_cast_for_database(column_value_with_enum_conversion))

              current_insert_line << "#{column_value_for_db}"

              unless column_index == columns_max_index
                current_insert_line << ','
              end

              column_index += 1
            end

            # override columns have already been converted to mysql string
            @override_column_values.values.each do |value|
              current_insert_line << "#{value}"

              unless column_index == columns_max_index
                current_insert_line << ','
              end

              column_index += 1
            end

            current_insert_line << ')'

            # check if the current mysql string is over the threshold. if so,
            # then we add a new string to the hash and allocate space for it
            # and start adding values to the new string. if we're under the
            # threshold, then we just add a comma and continue on filling this
            # string up.
            if insert_line_hash[insert_line_hash_index].size > STRING_SIZE_THRESHOLD
              insert_line_hash_index += 1
              insert_line_hash[insert_line_hash_index] = String.new(ActiveRecord::BulkOps.comparable_ruby_version > "020600" ? {capacity: ALLOCATED_STRING_SIZE} : {})
              current_insert_line = insert_line_hash[insert_line_hash_index]
              current_insert_line << insert_line_prefix
            elsif item_index != item_max_index
              current_insert_line << ','
            end
          end

          @insert_line_hash = insert_line_hash
        end
      end
    end
  end
end
