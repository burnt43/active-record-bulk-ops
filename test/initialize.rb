module Warning
  def warn(msg)
    # NoOp
  end
end

require 'minitest/pride'
require 'minitest/autorun'

require 'active_record'
require 'mini_record'
require './lib/active-record-bulk-ops'

ActiveRecord::Base.establish_connection(
  adapter:  'mysql2',
  host:     'localhost',
  username: 'active_record_bulk_ops_tester',
  password: 'f1re*sp3llz',
  database: 'active_record_bulk_ops_test'
)

module ActiveRecord
  module BulkOps
    module Testing
      class << self
        def models
          @models ||= [ActiveRecord::BulkOps::Testing::Foo]
        end

        def create_tables!
          models.each do |klass|
            klass.auto_upgrade!
          end
        end

        def destroy_tables!
          models.each do |klass|
            ActiveRecord::Base.connection.execute("DROP TABLE #{klass.table_name};")
          end
        end
      end

      class Test < Minitest::Test
      end

      class Foo < ActiveRecord::Base
        # fields
        field :enum01,   as: :integer
        field :int01,    as: :integer
        field :string01, as: :string

        # enums
        enum enum01: {
          value01: 100,
          value02: 200,
          value03: 300
        }
      end
    end
  end
end

ActiveRecord::BulkOps::Testing.create_tables!

MiniTest.after_run do
  ActiveRecord::BulkOps::Testing.destroy_tables!
end

